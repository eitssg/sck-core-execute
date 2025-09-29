import time
from typing import Any

from pydantic import ValidationError

import core_logging as log
import core_framework as util
import core_helper.aws as aws

from core_framework.models import TaskPayload

from .actionlib.helper import Helper

from .execute import (
    run_state_machine,
    timeout_imminent,
    load_actions,
    load_state,
    save_state,
)


def handler(event: dict, context: Any | None = None) -> dict:
    """
    Receive an "Actions" event request and execute it.

    This function is the main entry point for Lambda execution within AWS Step Functions.
    It processes the incoming event, loads actions and state from S3, creates an action
    helper, and runs the state machine execution loop until completion or timeout.

    :param event: The Lambda event from Step Functions containing TaskPayload data.
                  This should be a dictionary that can be parsed into a TaskPayload object.
    :type event: dict
    :param context: Lambda context providing runtime information such as remaining
                    execution time, memory limits, and other Lambda runtime details.
                    This is used to determine when timeout is imminent for long-running actions.
    :type context: Any | None
    :return: A dictionary containing the TaskPayload with updated flow_control state.
             The flow_control will be one of "execute", "success", or "failure".
    :rtype: dict
    :raises Exception: If event parsing fails or critical errors occur during execution.

    Example:
        >>> # Step Functions event with TaskPayload data
        >>> event = {
        ...     "task": "my-task",
        ...     "actions": {...},
        ...     "state": {...},
        ...     "flow_control": execute,
        ... }
        >>> result = handler(event, lambda_context)
        >>> print(f"Execution result: {result['flow_control']}")

    Note:
        The function automatically sets flow_control to "execute" if not provided
        in the event. This handler is designed to work with AWS Step Functions
        for orchestrating long-running task execution.
    """
    log.trace("Entering core_execute.handler")

    try:
        # Task payload is a model object should have been created with TaskPayload.model_dump()
        task_payload = TaskPayload.model_validate(event)

        log.set_correlation_id(task_payload.correlation_id)  # type: ignore

        log.setup(task_payload.identity)

        log.debug("Entering Execute handler for task: {}", task_payload.task)
        log.debug("Execute Event: ", details=task_payload.model_dump())

        # Load actions from the S3 bucket "{task}.actions"
        log.debug("Loading actions for task: {}", task_payload.task)
        actions = load_actions(task_payload)
        log.debug("Loaded {} action actions", len(actions))

        # Load state - this should have been a document created from "get_facts" for Jinja2 rendering
        log.debug("Loading state for task: {}", task_payload.task)
        context_state = load_state(task_payload)

        run_count = int(context_state.get("system/run_count", 0))
        run_count += 1
        context_state["system/run_count"] = run_count

        log.debug("Loaded state with {} keys", len(context_state.keys()))

        # Create action helper with loaded actions and state
        action_helper = Helper(actions, context_state, task_payload)

        # Execute state machine - designed for Step Functions
        # Instead of a tight loop, do limited iterations
        #
        # # Prevent runaway loops.  Will iterate once for each action if all
        # are dependent on each other plus a few extra

        iteration = 0

        # Tell the state machine to start executing if not already set
        if not task_payload.flow_control or task_payload.flow_control == "init":
            task_payload.flow_control = "execute"

        while task_payload.flow_control == "execute" and not timeout_imminent(context):

            iteration += 1
            log.debug("State machine iteration {}", iteration)

            task_payload.flow_control = run_state_machine(action_helper)

            # Pause briefly to allow other processes to run
            # Jobs may be running in other threads.  Yield to them and loop back to check up on the status

            if timeout_imminent(context):
                log.warning("Execution stopped due to timeout, Step Functions will retry")

            time.sleep(1)

        # Save state back to S3
        log.debug("Saving state for task: {}", task_payload.task)

        context_state["flow_control"] = task_payload.flow_control
        save_state(task_payload, context_state)

        log.debug("Exiting handler with flow_control state: {}", task_payload.flow_control)
        log.debug("Execution completed after {} loops", iteration)

        result = task_payload.model_dump()

        log.trace("Handler result: ", details=result)

        return result

    except Exception as e:

        validation_errors = []
        errortype = type(e).__name__

        if isinstance(e, ValidationError):
            message = f"Validation error parsing event into TaskPayload ({errortype}): {e.title}"
            # Handle validation errors specifically
            log.error("Validation error parsing event into TaskPayload: {}", e)
            for error in e.errors():
                # detail the pydantic validation error
                validation_errors.append(
                    {
                        "loc": error.get("loc", []),
                        "msg": error.get("msg", ""),
                        "type": error.get("type", ""),
                        "input": error.get("input", None),
                    }
                )
        else:
            message = f"Error parsing event into TaskPayload ({errortype}): {str(e)}"

        error_details: dict[str, Any] = {"Message": message}
        if validation_errors:
            error_details["ValidationErrors"] = validation_errors

        log.error("Error in handler execution", details=error_details)
        log.error("Original event: ", details=event)

        return {"FlowControl": "failure"}


def invoke_execute_handler(task_payload: TaskPayload) -> None:
    """
    Invoke the execute handler for a given TaskPayload.

    This function is used to trigger the execution of the handler logic
    for a specific task payload, allowing it to process actions and state.

    :param task_payload: The TaskPayload object containing task details and state.
    :type task_payload: TaskPayload
    """
    log.debug("Invoking execute handler for task: {}", task_payload.task)

    if util.is_local_mode():
        # Call the main handler function with the task payload
        response = handler(task_payload.model_dump())
    else:
        arn = util.get_execute_lambda_arn()

        if not arn:
            raise Exception("Cannot determine execute Lambda ARN.  Is the environment configured correctly?")

        log.debug("Invoking Lambda function: {}", arn)

        aws.invoke_lambda(
            arn=arn,
            request_payload=task_payload.model_dump(),
            role_arn=util.get_provisioning_role_arn(),
            invocation_type="Event",  # Use Event for async execution
        )

    log.debug("Handler response: ", details=response)
