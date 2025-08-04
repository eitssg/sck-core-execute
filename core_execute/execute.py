# Action runner execution engine
#
from typing import Any
from datetime import datetime, timezone
import io
import inflect

import core_logging as log

import core_framework as util
from core_framework.models import TaskPayload, ActionSpec
from core_helper.magic import MagicS3Client

from .actionlib.helper import Helper, FlowControl

_p = inflect.engine()

# When the lambda function is booted and the python module is loaded, we'll get a __bootup_time__
__bootup_time__ = datetime.now(timezone.utc).timestamp()

__max_runtime__ = 10 * 60 * 1000  # 10 minutes in milliseconds


def timeout_imminent(context: Any | None = None) -> bool:
    """
    Check if the Lambda function is about to timeout.

    The function considers timeout imminent if less than 10 seconds remaining.
    If no context is provided, it assumes we're not running in a Lambda environment
    and calculates based on the maximum runtime of 10 minutes from bootup time.

    :param context: Lambda context object providing runtime information
    :type context: Any | None
    :return: True if the Lambda function is about to timeout, False otherwise
    :rtype: bool

    Example:
        >>> # In Lambda environment
        >>> if timeout_imminent(context):
        ...     log.warning("Lambda timeout imminent!")
        >>>
        >>> # Outside Lambda environment
        >>> if timeout_imminent():
        ...     log.warning("Process timeout imminent!")
    """
    # Timeout threshold is 10 seconds (in milliseconds)
    timeout_threshold_ms = 10000

    # Check if we're running in Lambda environment
    if context and hasattr(context, "get_remaining_time_in_millis"):
        # Lambda environment - use the actual remaining time
        remaining_time_in_millis = context.get_remaining_time_in_millis()
        log.trace("Lambda context remaining time: {} ms", remaining_time_in_millis)

    else:
        # Local/standalone mode - emulate get_remaining_time_in_millis()
        current_time = datetime.now(timezone.utc).timestamp()
        elapsed_time_ms = int((current_time - __bootup_time__) * 1000)
        remaining_time_in_millis = __max_runtime__ - elapsed_time_ms

        log.trace(
            "Local mode - elapsed: {} ms, remaining: {} ms",
            elapsed_time_ms,
            remaining_time_in_millis,
        )

        # Ensure we don't return negative values
        if remaining_time_in_millis < 0:
            remaining_time_in_millis = 0

    # Consider timeout imminent if less than threshold remaining
    is_imminent = remaining_time_in_millis < timeout_threshold_ms

    if is_imminent:
        log.debug(
            "Timeout imminent: {} ms remaining (threshold: {} ms)",
            remaining_time_in_millis,
            timeout_threshold_ms,
        )

    return is_imminent


def __get_next_status(action_helper: Helper) -> FlowControl:
    """
    Internal function to determine the next state of the action execution state machine.

    This function analyzes the current state of all actions and determines what the
    next execution state should be based on the number of runnable, running, pending,
    completed, and incomplete actions.

    :param action_helper: The Helper object containing all action information
    :type action_helper: Helper
    :return: The next state - one of "execute", "failure", or "success"
    :rtype: str

    Example:
        >>> helper = Helper(actions, state)
        >>> next_state = __run_state_machine(helper)
        >>> if next_state == "execute":
        ...     log.info("More actions to execute")
    """
    runnable_actions = action_helper.runnable_actions()
    running_actions = action_helper.running_actions()
    pending_actions = action_helper.pending_actions()
    completed_actions = action_helper.completed_actions()
    incomplete_actions = action_helper.incomplete_actions()

    log.info(
        "Status: {} complete ({} running, {} runnable, {} pending, {} completed, {} incomplete)",
        _percentage(
            len(completed_actions),
            len(completed_actions) + len(incomplete_actions),
        ),
        len(running_actions),
        len(runnable_actions),
        len(pending_actions),
        len(completed_actions),
        len(incomplete_actions),
        details={
            "RunningActions": [a.name for a in running_actions],
            "RunnableActions": [a.name for a in runnable_actions],
        },
    )

    if len(runnable_actions) > 0:
        log.info(
            "Found {}, re-entering execution",
            _pluralise("runnable action", len(runnable_actions)),
            details={"RunnableActions": [a.name for a in runnable_actions]},
        )
        # Execute runnable actions
        return FlowControl.EXECUTE

    elif len(running_actions) > 0:
        log.info(
            "Waiting for {} to complete",
            _pluralise("running action", len(running_actions)),
            details={
                "RunningActions": [a.name for a in running_actions],
            },
        )
        # Execute executing actions
        return FlowControl.EXECUTE

    elif len(runnable_actions) == 0 and len(running_actions) == 0 and len(pending_actions) > 0:
        # No runnable or running actions, but still have actions pending - pending actions will never be runnable
        log.error(
            "Found {}",
            _pluralise("unrunablle action", len(pending_actions)),
            details={"UnrunnableActions": [a.name for a in pending_actions]},
        )
        # 1 or more actions have failed (nothing is running)
        return FlowControl.FAILURE

    else:
        # All actions are completed successfully
        return FlowControl.SUCCESS


def run_state_machine(action_helper: Helper, context: Any | None) -> FlowControl:
    """
    Execute the main state machine for action processing.

    This function orchestrates the execution of actions by:
    1. Checking for failed actions and returning failure immediately
    2. Updating the status of running actions
    3. Executing runnable actions until timeout is imminent
    4. Determining the next state based on current action states

    The function is designed to work with AWS Step Functions and will return
    appropriate states for the Step Functions state machine to handle.

    :param action_helper: The Helper object containing all action information
    :type action_helper: Helper
    :param context: Lambda context object providing runtime information
    :type context: Any | None
    :return: The execution result state - one of "execute", "failure", or "success"
    :rtype: str

    Example:
        >>> helper = Helper(actions, state)
        >>> result = run_state_machine(helper, lambda_context)
        >>> if result == FlowControl.SUCCESS:
        ...     log.info("All actions completed successfully")
    """
    log.trace("Entering run_state_machine")

    # First, check if there are any failed actions - fail fast
    failed_actions = action_helper.failed_actions()
    if len(failed_actions) > 0:
        log.error(
            "Found {} failed actions: {}",
            len(failed_actions),
            [a.name for a in failed_actions],
        )
        return FlowControl.FAILURE

    # Track progress for logging
    actions_processed = 0
    actions_executed = 0

    # Update the status of running actions
    running_actions = action_helper.running_actions()
    log.debug("Checking status of {} running actions", len(running_actions))

    # Any action that is running has run but not completed are 'running'
    for action in running_actions:
        if timeout_imminent(context):
            log.warning("Timeout imminent, stopping action status checks")
            break

        actions_processed += 1
        log.trace("Checking status of running action: {}", action.name)

        try:
            # Check completion of action
            action.check()

            if action.is_failed():
                log.error("Action {} failed during status check", action.name)
                return FlowControl.FAILURE
            elif action.is_complete():
                log.info("Action {} completed successfully", action.name)

        except Exception as e:
            log.error("Error checking status of action {}: {}", action.name, e)
            return FlowControl.FAILURE

    # Execute runnable actions.  Thise that were PENDING or INCOMPLETE
    runnable_actions = action_helper.runnable_actions()
    log.debug("Found {} runnable actions", len(runnable_actions))

    for action in runnable_actions:
        if timeout_imminent(context):
            log.warning("Timeout imminent, stopping action execution")
            break

        actions_processed += 1
        log.info("Executing action: {}", action.name)

        try:
            # Execute the action
            action.execute()
            actions_executed += 1

            if action.is_complete():
                log.info("Action {} completed immediately", action.name)
            elif action.is_running():
                log.info("Action {} is still running", action.name)
            elif action.is_failed():
                log.error("Action {} failed during execution", action.name)
                return FlowControl.FAILURE

        except Exception as e:
            log.error("Error executing action {}: {}", action.name, e)
            return FlowControl.FAILURE

    log.debug(
        "Processed {} actions ({} executed) in this iteration",
        actions_processed,
        actions_executed,
    )

    # Determine next state based on current action states
    next_state = __get_next_status(action_helper)

    log.debug("State machine determined next state: {}", str(next_state))

    return next_state


def _pluralise(phrase: str, l: int):
    return f"{l} {_p.plural(phrase, l)}"


def _percentage(top, bottom):
    """
    Calculate percentage as an integer string.

    :param top: The numerator value
    :type top: int | float
    :param bottom: The denominator value
    :type bottom: int | float
    :return: The percentage as a string with % symbol
    :rtype: str

    Example:
        >>> _percentage(75, 100)
        "75%"
        >>> _percentage(10, 0)
        "100%"
    """
    if bottom == 0:
        return "100%"
    else:
        return "{}%".format(int(float(top) / float(bottom) * 100.0))


def load_actions(task_payload: TaskPayload) -> list[ActionSpec]:
    """
    Load ActionSpec definitions from S3.

    Downloads the actions file from S3 and parses it based on the content type.
    Supports both YAML and JSON formats. The content type is determined from
    the S3 object metadata.

    :param task_payload: The TaskPayload object containing actions details
    :type task_payload: TaskPayload
    :return: List of ActionSpec objects loaded from S3
    :rtype: list[ActionSpec]
    :raises ValueError: If no actions found in task payload or unknown content type
    :raises Exception: If S3 operation fails or data parsing fails

    Example:
        >>> payload = TaskPayload(actions=ActionDetails(...))
        >>> actions = load_actions(payload)
        >>> for action in actions:
        ...     print(f"Action: {action.name}")
    """
    # Load actions and create an action helper object
    log.trace("Loading actions")

    if task_payload.package and task_payload.package.deployspec and task_payload.package.deployspec.actions:
        log.debug(
            "Using {} actions from package",
            len(task_payload.package.deployspec.actions),
        )
        return task_payload.package.deployspec.actions

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

        # we mutate the actions details.  bad on us.
        actions_details.content_type = content_type

        log.debug("Loaded Actions Content Type: {}", content_type)
        log.debug("Loaded Actions Data: ", details=actions_data)

        if actions_data is None:
            log.trace("Actions file was empty or null, returning empty list")
            return []

        actions: list[ActionSpec] = [ActionSpec(**action) for action in actions_data]

        log.trace("Actions loaded successfully")
        return actions

    except Exception as e:
        log.error("Failed to parse actions data with content type {}: {}", content_type, e)
        raise Exception(f"Failed to parse actions data: {str(e)}") from e


def save_actions(task_payload: TaskPayload, actions: list[ActionSpec]) -> None:
    """
    Save ActionSpec definitions to S3.

    Serializes the list of ActionSpec objects and saves them to S3 as YAML format.
    Updates the version_id in the task payload with the new S3 object version.

    :param task_payload: The TaskPayload object containing actions details
    :type task_payload: TaskPayload
    :param actions: List of ActionSpec objects to save
    :type actions: list[ActionSpec]
    :raises ValueError: If no actions file definition found in task payload
    :raises TypeError: If actions contains non-ActionSpec objects
    :raises Exception: If S3 operation fails or data serialization fails

    Example:
        >>> payload = TaskPayload(actions=ActionDetails(...))
        >>> actions = [ActionSpec(name="test", kind="AWS::Operation", params={...})]")]
        >>> save_actions(payload, actions)
    """
    actions_details = task_payload.actions
    if not actions_details:
        raise ValueError("No actions file definition found in the task payload")

    data: list[dict] = []
    for action in actions:
        if isinstance(action, ActionSpec):
            data.append(action.model_dump())
        else:
            raise TypeError(f"Expected ActionSpec, got {type(action)}")

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

        actions_details.version_id = response.version_id

        log.trace("Actions saved successfully to S3")

    except Exception as e:
        log.error(
            "Failed to save actions to S3 bucket {} key {}: {}",
            actions_details.bucket_name,
            actions_details.key,
            e,
        )
        raise Exception(f"Failed to save actions to S3: {str(e)}") from e

    log.trace("Exit Save actions")


def load_state(task_payload: TaskPayload) -> dict:
    """
    Load the execution state from S3.

    Downloads and parses the state file from S3. The state data is a dictionary
    containing facts and execution state information. Supports both YAML and JSON
    formats based on the content type.

    :param task_payload: The TaskPayload object containing state details
    :type task_payload: TaskPayload
    :return: Dictionary containing the execution state data
    :rtype: dict
    :raises ValueError: If no state found in task payload
    :raises Exception: If state file has unknown content type or S3 operation fails

    Example:
        >>> payload = TaskPayload(state=StateDetails(...))
        >>> state = load_state(payload)
        >>> print(f"Current state: {state}")
    """
    log.trace("Loading state")

    state_details = task_payload.state
    if state_details is None:
        raise ValueError("No state found in the task payload")

    if state_details.version_id == "new":
        log.info("Creating new state")
        return {}

    extra_args = {}
    if state_details.version_id is not None:
        extra_args["VersionId"] = state_details.version_id

    log.info("Loading state from {}", state_details.get_full_path())

    try:
        # Retrieve state from S3 (or the magic bucket (could be Local))
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
        # read yaml content if context type is yaml
        if util.is_yaml_mimetype(content_type):
            state = util.read_yaml(state_fileobj)
        # read json content if context type is json
        elif util.is_json_mimetype(content_type):
            state = util.read_json(state_fileobj)
        else:
            raise Exception(f"State file unknown content type: {content_type}")

        log.debug("Loaded State Content Type: {}", content_type)
        log.debug("Loaded State Data: ", details=state)

        if state is None:
            log.trace("State file was empty or null, returning empty dict")
            return {}

        log.trace("State loaded successfully")
        return state

    except Exception as e:
        log.error("Failed to parse state data with content type {}: {}", content_type, e)
        raise Exception(f"Failed to parse state data: {str(e)}") from e


def save_state(task_payload: TaskPayload, state: dict) -> None:
    """
    Save the execution state to S3.

    Serializes the state dictionary and saves it to S3. The format (YAML or JSON)
    is determined by the content type in the state details. Updates the version_id
    in the task payload with the new S3 object version.

    :param task_payload: The TaskPayload object containing state details
    :type task_payload: TaskPayload
    :param state: Dictionary containing the execution state to save
    :type state: dict
    :raises ValueError: If no state found in task payload
    :raises Exception: If S3 operation fails or data serialization fails

    Example:
        >>> payload = TaskPayload(state=StateDetails(...))
        >>> state = {"current_step": "executing", "completed_actions": ["action1"]}
        >>> save_state(payload, state)
    """
    log.trace("Enter Save state")

    state_details = task_payload.state
    if state_details is None:
        raise ValueError("No state found in the task payload")

    content_type = state_details.content_type or "application/x-yaml"

    log.debug("Saving State Content Type: {}", content_type)
    log.debug("Saving State Data: ", details=state)

    try:
        if util.is_yaml_mimetype(content_type):
            result_data = util.to_yaml(state)
        elif util.is_json_mimetype(content_type):
            result_data = util.to_json(state)
        else:
            raise ValueError(f"Unsupported content type for state serialization: {content_type}")
    except Exception as e:
        log.error("Failed to serialize state data with content type {}: {}", content_type, e)
        raise Exception(f"Failed to serialize state data: {str(e)}") from e

    log.info("Save state to {}", state_details.key)

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

        state_details.version_id = response.version_id

        log.trace("State saved successfully to S3")

    except Exception as e:
        log.error(
            "Failed to save state to S3 bucket {} key {}: {}",
            state_details.bucket_name,
            state_details.key,
            e,
        )
        raise Exception(f"Failed to save state to S3: {str(e)}") from e

    log.trace("Exit Save state")
