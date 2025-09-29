"""Action execution engine with rerun support and lifecycle hooks.

Provides the core orchestration for Simple Cloud Kit:
- Dependency-aware action scheduling
- Parallel execution where safe
- Rerun support via FlowControl=INIT
- Critical vs. non-critical failure handling
- Robust logging and error handling
"""

from typing import Protocol
import io

import core_logging as log
import core_framework as util

from core_framework.models import TaskPayload, ActionResource
from core_framework.constants import CTX_CONTEXT

from core_helper.magic import MagicS3Client

from core_db.facter import get_facts

from .actionlib.helper import Helper


class HasMillis(Protocol):
    def get_remaining_time_in_millis(self) -> int: ...  # noqa: E704


def timeout_imminent(context: HasMillis | None = None) -> bool:
    """Return True if the Lambda invocation is close to timing out.

    Uses context.get_remaining_time_in_millis() when available (real Lambda/Step
    Functions). If no context or method is available (local runs), returns False.

    Threshold: timeout is considered imminent when less than 10 seconds remain.

    Args:
        context: The Lambda context object, if provided.

    Returns:
        True if fewer than 10,000 ms remain; otherwise False.
    """
    # Timeout threshold is 10 seconds (in milliseconds)
    timeout_threshold_ms: int = 10000

    # Check if we're running in Lambda environment
    if context and hasattr(context, "get_remaining_time_in_millis") and callable(context.get_remaining_time_in_millis):
        # Lambda environment - use the actual remaining time
        remaining_time_in_millis = context.get_remaining_time_in_millis()
        log.trace("Lambda context remaining time: {} ms", remaining_time_in_millis)
    else:
        return False

    # Consider timeout imminent if less than threshold remaining
    is_imminent = remaining_time_in_millis < timeout_threshold_ms

    if is_imminent:
        log.debug(
            "Timeout imminent: {} ms remaining (threshold: {} ms)",
            remaining_time_in_millis,
            timeout_threshold_ms,
        )

    return is_imminent


def run_state_machine(helper: Helper) -> str:
    """Run the execution loop and return the next FlowControl value.

    Schedules and runs actions based on their current status and dependencies.
    - If any action was started or continued this pass, returns "execute".
    - If none ran and no failures exist, returns "success".
    - If none ran and failures exist, returns "failure".

    Args:
        helper: The Helper coordinating actions, dependencies, and threading.

    Returns:
        The FlowControl value for the Step Function: "execute", "success", or "failure".
    """
    log.trace("Entering enhanced run_state_machine (threading={})", helper.use_threading)

    def check_action(action: ActionResource) -> bool:

        if helper.is_action_complete(action):
            log.debug("Action {} complete.", action.action_name)
            return False

        if helper.is_action_failed(action):
            log.debug("Action {} failed.", action.action_name)
            return False

        if helper.is_action_running(action):
            log.debug("Action {} is running", action.action_name)
            helper.run_action_execute(action)
            return True

        if helper.is_action_pending(action):
            log.debug("Action {} is pending", action.action_name)
            helper.run_action_execute(action)
            return True

        return False

    ran_something = any(check_action(action) for action in helper.actions_resources)

    if ran_something:
        return "execute"

    return "success" if helper.number_failed == 0 else "failure"


# S3 Operations - Load and Save Functions


def load_actions(task_payload: TaskPayload) -> list[ActionResource]:
    """Load ActionResource definitions from S3 or the embedded package.

    Order of precedence:
    1) Embedded actions in the task payload package
    2) Actions file from S3 (YAML or JSON)

    Args:
        task_payload: The task payload with actions metadata (S3 location, content type).

    Returns:
        A list of ActionResource objects.

    Raises:
        ValueError: If no actions are defined in the task payload.
        Exception: If S3 download or parsing fails, or content type is unknown.
    """
    log.trace("Loading actions")

    # Check if actions are embedded in the package first (takes priority)
    if task_payload.package and task_payload.package.actions:
        log.debug("Using {} actions from embedded package", len(task_payload.package.actions))
        return task_payload.package.actions

    # Load actions from S3
    actions_details = task_payload.actions
    if actions_details is None:
        raise ValueError("No actions found in the task payload")

    bucket_name = actions_details.bucket_name
    bucket_region = actions_details.bucket_region

    log.info("Downloading actions from {}", actions_details.key)

    try:
        s3_client = MagicS3Client.get_client(Region=bucket_region)

        actions_fileobj = io.BytesIO()
        download_details: dict = s3_client.download_fileobj(Bucket=bucket_name, Key=actions_details.key, Fileobj=actions_fileobj)

        content_type = download_details.get("ContentType", "application/x-yaml")
        version_id = download_details.get("VersionId", None)

        log.debug(
            "Actions download successful",
            details={
                "bucket": bucket_name,
                "key": actions_details.key,
                "version_id": version_id,
                "content_type": content_type,
            },
        )

    except Exception as e:
        log.error(
            "Failed to download actions from S3 bucket {} key {}: {}",
            bucket_name,
            actions_details.key,
            e,
        )
        raise Exception(f"Failed to load actions from S3: {str(e)}") from e

    try:
        if util.is_yaml_mimetype(content_type):
            actions_data = util.read_yaml(actions_fileobj)
        elif util.is_json_mimetype(content_type):
            actions_data = util.read_json(actions_fileobj)
        else:
            raise ValueError(f"Actions file unknown content type: {content_type}")

        # Update actions details with content type
        actions_details.content_type = content_type

        log.debug("Loaded Actions Content Type: {}", content_type)
        log.debug("Loaded Actions Data: ", details=actions_data)

        if actions_data is None:
            log.trace("Actions file was empty or null, returning empty list")
            return []

        # Convert to ActionResource objects
        actions: list[ActionResource] = [ActionResource(**action) for action in actions_data]

        log.trace("Actions loaded successfully ({} actions)", len(actions))
        return actions

    except Exception as e:
        log.error("Failed to parse actions data with content type {}: {}", content_type, e)
        raise Exception(f"Failed to parse actions data: {str(e)}") from e


def load_state(task_payload: TaskPayload) -> dict:
    """Load the execution state from S3 (or create a new one).

    Downloads and parses the state file based on content type (YAML/JSON). If
    the state is marked as new (version_id == "new"), an empty dict is returned.

    Args:
        task_payload: The task payload with state metadata (S3 location, version).

    Returns:
        The state dictionary; empty if the state is new or file is empty.

    Raises:
        ValueError: If no state is defined in the task payload.
        Exception: If S3 download, parsing fails, or content type is unknown.
    """
    log.trace("Loading state")

    state_details = task_payload.state
    if state_details is None:
        raise ValueError("No state found in the task payload")

    # Handle new state creation
    if state_details.version_id == "new":
        log.info("Creating new state (no existing state file)")
        return {}

    # Prepare S3 request parameters
    extra_args = {}
    if state_details.version_id is not None:
        extra_args["VersionId"] = state_details.version_id

    log.info("Loading state from {}", state_details.get_full_path())

    try:
        # Retrieve state from S3 (or magic bucket for local development)
        s3_client = MagicS3Client.get_client(Region=state_details.bucket_region)

        state_fileobj = io.BytesIO()
        state_download_response = s3_client.download_fileobj(
            Bucket=state_details.bucket_name,
            Key=state_details.key,
            Fileobj=state_fileobj,
            ExtraArgs=extra_args,
        )

        content_type = state_download_response.get("ContentType", "application/x-yaml")
        state_details.content_type = content_type
        version_id = state_download_response.get("VersionId", None)
        state_details.version_id = version_id

        log.debug(
            "State download successful",
            details={
                "bucket": state_details.bucket_name,
                "key": state_details.key,
                "version_id": version_id,
                "content_type": content_type,
            },
        )

    except Exception as e:
        log.error(
            "Failed to download state from S3 bucket {} key {} version {}: {}",
            state_details.bucket_name,
            state_details.key,
            state_details.version_id,
            e,
        )
        raise Exception(f"Failed to load state from S3: {str(e)}") from e

    try:
        # Parse state data based on content type
        if util.is_yaml_mimetype(content_type):
            state = util.read_yaml(state_fileobj)
        elif util.is_json_mimetype(content_type):
            state = util.read_json(state_fileobj)
        else:
            raise Exception(f"State file unknown content type: {content_type}")

        if not state:
            log.trace("State file was empty or null, initializing empty state")
            state = {}

        log.debug("Loaded State Content Type: {}", content_type)
        log.debug("Loaded State Data: ", details=state)

        # Load new Facts for this run from the DB and set the state
        facts = _get_state_facts(task_payload)

        # Facts go into the "context" section of state, and this happens to be 'lowercase"
        state[CTX_CONTEXT] = facts

        log.trace("State loaded successfully ({} keys)", len(state.keys()) if state else 0)

        return state

    except Exception as e:
        log.error("Failed to parse state data with content type {}: {}", content_type, e)
        raise Exception(f"Failed to parse state data: {str(e)}") from e


def _get_state_facts(task_payload: TaskPayload) -> dict:
    """Create an initial state dictionary based on the task payload.

    Populates facts such as task name, correlation ID, and identity when
    available via core_db.facter.get_facts.

    Args:
        task_payload: The task payload containing metadata for fact generation.

    Returns:
        A dictionary of initial state facts, or an empty dict if none are available.
    """
    log.trace("Generating initial state facts")

    try:
        facts = get_facts(task_payload.deployment_details)
        if not facts:
            log.trace("No initial state facts generated")
            facts = {}
    except Exception as e:
        log.error("Failed to generate initial state facts: {}", e)
        facts = {}

    log.trace("Initial state facts generated with {} keys", len(facts))

    return facts


def save_state(task_payload: TaskPayload, state: dict) -> None:
    """Serialize and save the execution state to S3.

    Serialization format (YAML or JSON) is determined by state_details.content_type.
    Updates the state version_id with the value returned by S3.

    Args:
        task_payload: The task payload containing the target S3 location.
        state: The state dictionary to persist.

    Raises:
        ValueError: If the task payload lacks state details or content type is unsupported.
        Exception: If serialization or S3 upload fails.
    """
    log.trace("Saving state")

    state_details = task_payload.state
    if state_details is None:
        raise ValueError("No state found in the task payload")

    content_type = state_details.content_type or "application/x-yaml"

    log.debug("Saving State Content Type: {}", content_type)
    log.debug("Saving State Data: ", details=state)

    try:
        # Serialize state data based on content type
        if util.is_yaml_mimetype(content_type):
            result_data = util.to_yaml(state)
        elif util.is_json_mimetype(content_type):
            result_data = util.to_json(state)
        else:
            raise ValueError(f"Unsupported content type for state serialization: {content_type}")

    except Exception as e:
        log.error("Failed to serialize state data with content type {}: {}", content_type, e)
        raise Exception(f"Failed to serialize state data: {str(e)}") from e

    log.info("Saving state to {}", state_details.key)

    try:
        s3_client = MagicS3Client.get_client(Region=state_details.bucket_region)

        response = s3_client.put_object(
            Bucket=state_details.bucket_name,
            Key=state_details.key,
            Body=result_data,
            ContentType=content_type,
            ServerSideEncryption="AES256",
        )

        log.debug("State save response: ", details=response)

        # Update version ID for future references
        state_details.version_id = response.version_id

        log.trace("State saved successfully to S3 (version: {})", response.version_id)

    except Exception as e:
        log.error(
            "Failed to save state to S3 bucket {} key {}: {}",
            state_details.bucket_name,
            state_details.key,
            e,
        )
        raise Exception(f"Failed to save state to S3: {str(e)}") from e

    log.trace("State save complete")


def save_actions(task_payload: TaskPayload, actions: list[ActionResource]) -> None:
    """Serialize and save ActionResource definitions to S3 (YAML).

    Primarily used for debugging or workflows that modify actions during execution.
    Updates the actions version_id with the value returned by S3.

    Args:
        task_payload: The task payload containing the target S3 location.
        actions: The list of ActionResource objects to persist.

    Raises:
        ValueError: If the task payload lacks an actions file definition.
        TypeError: If the actions list contains non-ActionResource items.
        Exception: If serialization or S3 upload fails.
    """
    actions_details = task_payload.actions
    if not actions_details:
        raise ValueError("No actions file definition found in the task payload")

    # Convert ActionResource objects to dictionaries
    data: list[dict] = []
    for action in actions:
        if isinstance(action, ActionResource):
            data.append(action.model_dump())
        else:
            raise TypeError(f"Expected ActionResource, got {type(action)}")

    content_type = actions_details.content_type or "application/x-yaml"

    log.debug("Saving Actions Content Type: {}", content_type)
    log.debug("Saving Actions Data: ", details={"Actions": data})

    try:
        # Serialize the data to YAML format
        serialized_data = util.to_yaml(data)
        log.debug("Actions data serialized successfully")

    except Exception as e:
        log.error("Failed to serialize actions data to YAML: {}", e)
        raise Exception(f"Failed to serialize actions data: {str(e)}") from e

    try:
        s3_client = MagicS3Client.get_client(Region=actions_details.bucket_region, DataPath=actions_details.data_path)

        response = s3_client.put_object(
            Bucket=actions_details.bucket_name,
            Key=actions_details.key,
            Body=serialized_data,
            ContentType=actions_details.content_type,
            ServerSideEncryption="AES256",
        )

        log.debug("Actions save response: ", details=response)

        # Update version ID for future references
        actions_details.version_id = response.version_id

        log.trace("Actions saved successfully to S3 (version: {})", response.version_id)

    except Exception as e:
        log.error(
            "Failed to save actions to S3 bucket {} key {}: {}",
            actions_details.bucket_name,
            actions_details.key,
            e,
        )
        raise Exception(f"Failed to save actions to S3: {str(e)}") from e
