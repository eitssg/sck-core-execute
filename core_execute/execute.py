# Action runner execution engine
#
from typing import Any
from datetime import datetime, timezone

import io
import json

from ruamel.yaml import YAML

import core_logging as log

from core_framework.constants import V_LOCAL
from core_framework.models import TaskPayload, ActionDefinition
from core_framework.magic import MagicS3Client

import core_helper.aws as aws

from .actionlib.helper import Helper

# When the lambda function is booted and the python module is loaded, we'll get a __bootup_time__
__bootup_time__ = datetime.now(timezone.utc).timestamp()

__maxx_runtime__ = 10 * 60 * 1000  # 10 minutes in milliseconds

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

    if context and hasattr(context, "get_remaining_time_in_millis"):
        remaining_time_in_millis = context.get_remaining_time_in_millis()
    else:
        # 10 minutes is the max time lambda will run
        t = datetime.now(timezone.utc).timestamp()
        remaining_time_in_millis = __maxx_runtime__ - int(t - __bootup_time__)

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
            "RunningActions": [a.label for a in running_actions],
            "RunnableActions": [a.label for a in runnable_actions],
        },
    )

    if len(runnable_actions) > 0:
        log.info(
            "Found {}, re-entering execution",
            _pluralise(runnable_actions, "runnable action"),
            details={"RunnableActions": [a.label for a in runnable_actions]},
        )
        return "execute"

    elif len(running_actions) > 0:
        log.info(
            "Waiting for {} to complete",
            _pluralise(running_actions, "running action"),
            details={
                "RunningActions": [a.label for a in running_actions],
            },
        )
        return "wait"

    elif (
        len(runnable_actions) == 0
        and len(running_actions) == 0
        and len(pending_actions) > 0
    ):
        # No runnable or running actions, but still have actions pending - pending actions will never be runnable
        log.error(
            "Found {}",
            _pluralise(pending_actions, "unrunnable action"),
            details={"UnrunnableActions": [a.label for a in pending_actions]},
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


def load_actions(task_payload: TaskPayload) -> list[ActionDefinition]:
    """
    Load the ActionDefinition Definitions from S3

    Args:
        actions_details (dict): ActionDefinition details.  The ActionDefinition Details object.

    Raises:
        Exception: Rais an exception if the content type is not recognized.

    Returns:
        dict: ActionDefinition definitions list
    """

    # Load actions and create an action helper object
    log.info("Loading actions")

    actions_details = task_payload.Actions
    if actions_details is None:
        raise ValueError("No actions found in the task payload")

    bucket_name = actions_details.BucketName
    bucket_region = actions_details.BucketRegion

    if task_payload.Package.Mode == V_LOCAL:
        ap = task_payload.Package.AppPath
        log.info("Downloading actions from file://{}/{}", ap, actions_details.Key)
        s3_actions_client = MagicS3Client(region=bucket_region, app_path=ap)
    else:
        log.info(
            "Downloading actions from s3://{}/{}", bucket_name, actions_details.Key
        )
        s3_actions_client = aws.s3_client(region=bucket_region)

    actions_fileobj = io.BytesIO()
    download_details = s3_actions_client.download_fileobj(
        Bucket=bucket_name, Key=actions_details.Key, Fileobj=actions_fileobj
    )

    content_type = download_details.get("ContentType", "application/x-yaml")

    if content_type == "application/x-yaml":
        actions_data = YAML(typ="safe").load(actions_fileobj)
    elif content_type == "application/json":
        actions_data = json.loads(actions_fileobj.getvalue())
    else:
        raise ValueError("Actions file unknown content type: {}", content_type)

    # we mutate the actions details.  bad on us.
    actions_details.ContentType = content_type

    if actions_data is None:
        return []

    actions: list[ActionDefinition] = [
        ActionDefinition(**action) for action in actions_data
    ]

    return actions


def load_state(task_payload: TaskPayload) -> dict:

    log.info("Downloading state from S3")

    state_details = task_payload.State
    if state_details is None:
        raise ValueError("No state found in the task payload")

    bucket_name = state_details.BucketName
    bucket_region = state_details.BucketRegion

    # Retrieve state from S3
    if state_details.VersionId == "new":
        log.info("Creating new state")
        return {}

    extra_args = {}
    if state_details.VersionId is not None:
        extra_args["VersionId"] = state_details.VersionId

    if task_payload.Package.Mode == V_LOCAL:
        ap = task_payload.Package.AppPath
        log.info("Downloading state from file://{}/{}", ap, state_details.Key)
        s3_state_client = MagicS3Client(region=bucket_region, app_path=ap)
    else:
        log.info("Downloading state from s3://{}/{}", bucket_name, state_details.Key)
        s3_state_client = aws.s3_client(region=bucket_region)

    state_fileobj = io.BytesIO()
    state_download_response = s3_state_client.download_fileobj(
        Bucket=bucket_name,
        Key=state_details.Key,
        Fileobj=state_fileobj,
        ExtraArgs=extra_args,
    )

    state_type = state_download_response.get("ContentType", "application/x-yaml")

    if state_type == "application/x-yaml":
        state = YAML(typ="safe").load(state_fileobj)
    elif state_type == "application/json":
        state = json.loads(state_fileobj.getvalue())
    else:
        raise Exception("State file unknown content type: {}", state_type)

    if state is None:
        return {}

    # we mutate the state details.  bad on us.
    state_details.ContentType = state_type

    return state


def save_state(task_payload: TaskPayload, state: dict) -> None:
    """ "
    Save the state to S3

    Args:
        state_details (dict): State details.  The State Details object of where to save the state
        state (dict): The state object to save.
    """

    log.info("Uploading state to S3")

    state_details = task_payload.State
    if state_details is None:
        raise ValueError("No state found in the task payload")

    bucket_name = state_details.BucketName
    bucket_region = state_details.BucketRegion

    content_type = state_details.ContentType or "application/x-yaml"

    if content_type == "application/x-yaml":
        y = YAML(typ="safe")
        y.allow_unicode = True
        y.default_flow_style = False
        data = io.StringIO()
        y.dump(state, data)
        result_data = data.getvalue()
    elif content_type == "application/json":
        result_data = json.dumps(state, indent=2)

    if task_payload.Package.Mode == V_LOCAL:
        ap = task_payload.Package.AppPath
        log.info("Save state to file://{}/{}", ap, state_details.Key)
        s3_state_client = MagicS3Client(region=bucket_region, app_path=ap)
    else:
        log.info("Save state to s3://{}/{}", bucket_name, state_details.Key)
        s3_state_client = aws.s3_client(region=bucket_region)

    response = s3_state_client.put_object(
        Bucket=bucket_name,
        Key=state_details.Key,
        Body=result_data,
        ContentType=content_type,
        ServerSideEncryption="AES256",
    )
    state_details.VersionId = response["VersionId"] if "VersionId" in response else None
