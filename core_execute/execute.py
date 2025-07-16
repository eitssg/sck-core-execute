# Action runner execution engine
#
from typing import Any
from datetime import datetime, timezone
import io
import json

import core_logging as log

import core_framework as util
from core_framework.models import TaskPayload, ActionSpec
from core_helper.magic import MagicS3Client

from .actionlib.helper import Helper

# When the lambda function is booted and the python module is loaded, we'll get a __bootup_time__
__bootup_time__ = datetime.now(timezone.utc).timestamp()

__max_runtime__ = 10 * 60 * 1000  # 10 minutes in milliseconds


def timeout_imminent(context: Any | None = None) -> bool:
    """
    Check if the Lambda function is about to timeout

    Args:
        context (dict): Lambda context object providing runtime information

    Returns:
        bool: True if the Lambda function is about to timeout, False otherwise
    """

    # Timeout is 10 seconds
    to = 10000

    # get_remaining_time_in_millis is available in the Lambda context
    # If context is None, we assume we're not running in a Lambda environment
    if context and hasattr(context, "get_remaining_time_in_millis"):
        remaining_time_in_millis = context.get_remaining_time_in_millis()
    else:
        # 10 minutes is the max time lambda will run
        t = datetime.now(timezone.utc).timestamp()
        remaining_time_in_millis = __max_runtime__ - int(t - __bootup_time__)

    # Consider timeout imminent if less than 10 seconds remaining
    return remaining_time_in_millis < to


def __run_state_machine(action_helper: Helper) -> str:

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
            _pluralise(runnable_actions, "runnable action"),
            details={"RunnableActions": [a.name for a in runnable_actions]},
        )
        return "execute"

    elif len(running_actions) > 0:
        log.info(
            "Waiting for {} to complete",
            _pluralise(running_actions, "running action"),
            details={
                "RunningActions": [a.name for a in running_actions],
            },
        )
        return "wait"

    elif len(runnable_actions) == 0 and len(running_actions) == 0 and len(pending_actions) > 0:
        # No runnable or running actions, but still have actions pending - pending actions will never be runnable
        log.error(
            "Found {}",
            _pluralise(pending_actions, "unrunnable action"),
            details={"UnrunnableActions": [a.name for a in pending_actions]},
        )
        return "failure"

    else:
        return "success"


def run_state_machine(action_helper: Helper, context: Any | None) -> str:  # noqa: C901
    failed = False

    # Check if there are any failed actions
    failed_actions = action_helper.failed_actions()
    if len(failed_actions) > 0:
        log.info("Found failed actions: {}", failed_actions)
        failed = True

    # Update the status of running actions
    if failed is False:
        for action in action_helper.running_actions():
            if timeout_imminent(context):
                break

            # Check completion of action
            action.check()
            if action.is_failed():
                failed = True
                break

    # Execute any runnable actions
    if not failed:
        for action in action_helper.runnable_actions():
            if timeout_imminent(context):
                break

            # Execute the action
            action.execute()
            if action.is_failed():
                failed = True
                break

    # Check for locked state
    if not failed:
        return __run_state_machine(action_helper)

    return "failure"


def _pluralise(items, phrase):
    return "{} {}{}".format(len(items), phrase, "" if len(items) == 1 else "s")


def _percentage(top, bottom):
    if bottom == 0:
        return "100%"
    else:
        return "{}%".format(int(float(top) / float(bottom) * 100.0))


def load_actions(task_payload: TaskPayload) -> list[ActionSpec]:
    """
    Load the ActionSpec Definitions from S3

    Args:
        actions_details (dict): ActionSpec details.  The ActionSpec Details object.

    Raises:
        Exception: Rais an exception if the content type is not recognized.

    Returns:
        dict: ActionSpec definitions list
    """

    # Load actions and create an action helper object
    log.trace("Loading actions")

    actions_details = task_payload.actions
    if actions_details is None:
        raise ValueError("No actions found in the task payload")

    bucket_name = actions_details.bucket_name
    bucket_region = actions_details.bucket_region

    s3_actions_client = MagicS3Client.get_client(Region=bucket_region)

    log.info("Downloading actions from {}", actions_details.key)

    actions_fileobj = io.BytesIO()
    download_details: dict = s3_actions_client.download_fileobj(
        Bucket=bucket_name, Key=actions_details.key, Fileobj=actions_fileobj
    )

    content_type = download_details.get("ContentType", "application/x-yaml")

    if content_type == "application/x-yaml":
        actions_data = util.read_yaml(actions_fileobj)
    elif content_type == "application/json":
        actions_data = json.loads(actions_fileobj.getvalue())
    else:
        raise ValueError("Actions file unknown content type: {}", content_type)

    # we mutate the actions details.  bad on us.
    actions_details.content_type = content_type

    log.debug("Loaded Actions Content Type: {}", content_type)
    log.debug("Loaded Actions Data: ", details=actions_data)

    if actions_data is None:
        return []

    actions: list[ActionSpec] = [ActionSpec(**action) for action in actions_data]

    log.trace("Actions loaded")

    return actions


def save_actions(task_payload: TaskPayload, specs: list[ActionSpec]) -> None:
    """
    Save the ActionSpec Definitions to S3
    Args:
        task_payload (TaskPayload): The TaskPayload object containing the actions details.
        actions (dict): The list of action definitions to save.
    """
    actions = task_payload.actions

    s3_actions_client = MagicS3Client.get_client(Region=actions.bucket_region, DataPath=actions.data_path)

    s3_actions_client.put_object(
        Bucket=actions.bucket_name,
        Key=actions.key,
        Body=util.to_yaml(specs),
        ContentType=actions.content_type,
    )


def load_state(task_payload: TaskPayload) -> dict:
    """
    Load the state from S3.  State data is a simple dictionary.  Facts data.

    Args:
        task_payload (TaskPayload): _description_

    Raises:
        ValueError: _description_
        Exception: _description_

    Returns:
        dict: _description_
    """
    log.trace("Loading state")

    state_details = task_payload.state
    if state_details is None:
        raise ValueError("No state found in the task payload")

    bucket_name = state_details.bucket_name
    bucket_region = state_details.bucket_region

    # Retrieve state from S3
    if state_details.version_id == "new":
        log.info("Creating new state")
        return {}

    extra_args = {}
    if state_details.version_id is not None:
        extra_args["VersionId"] = state_details.version_id

    log.info("Downloading state from {}", state_details.key)

    s3_state_client = MagicS3Client.get_client(Region=bucket_region)

    state_fileobj = io.BytesIO()
    state_download_response = s3_state_client.download_fileobj(
        Bucket=bucket_name,
        Key=state_details.key,
        Fileobj=state_fileobj,
        ExtraArgs=extra_args,
    )

    # assume the mimetype is application/x-yaml if not specified
    mimetype = state_download_response.get("ContentType") or "application/x-yaml"

    # read yaml content if context type is yaml
    if util.is_yaml_mimetype(mimetype):
        state = util.read_yaml(state_fileobj)
    # read json content if context type is json
    elif util.is_json_mimetype(mimetype):
        state = json.loads(state_fileobj.getvalue())
    else:
        raise Exception("State file unknown content type: {}", mimetype)

    # we mutate the state details.  bad on us.
    state_details.content_type = mimetype

    log.debug("Loaded State Content Type: {}", mimetype)
    log.debug("Loaded State Data: ", details=state)

    if state is None:
        return {}

    log.trace("State loaded")

    return state


def save_state(task_payload: TaskPayload, state: dict) -> None:
    """ "
    Save the state to S3

    Args:
        state_details (dict): State details.  The State Details object of where to save the state
        state (dict): The state object to save.
    """

    log.trace("Enter Save state")

    state_details = task_payload.state
    if state_details is None:
        raise ValueError("No state found in the task payload")

    bucket_name = state_details.bucket_name
    bucket_region = state_details.bucket_region

    content_type = state_details.content_type or "application/x-yaml"

    log.debug("Saving State Content Type: {}", content_type)
    log.debug("Saving State Data: ", details=state)

    if util.is_yaml_mimetype(content_type):
        result_data = util.to_yaml(state)
    elif util.is_json_mimetype:
        result_data = json.dumps(state, indent=2)

    log.info("Save state to {}", state_details.key)

    s3_state_client = MagicS3Client.get_client(Region=bucket_region)

    response = s3_state_client.put_object(
        Bucket=bucket_name,
        Key=state_details.key,
        Body=result_data,
        ContentType=content_type,
        ServerSideEncryption="AES256",
    )

    log.debug("State save response: ", details=response)

    state_details.version_id = response.version_id

    log.trace("Exit Save state")
