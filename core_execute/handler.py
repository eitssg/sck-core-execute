import time
from typing import Any

import core_logging as log
import core_framework as util
import core_helper.aws as aws

from core_framework.models import TaskPayload

from .actionlib.helper import Helper, FlowControl

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
        ...     "flow_control": FlowControl.EXECUTE.value,
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
        task_payload = TaskPayload(**event)

        log.setup(task_payload.identity)
        log.info("Entering handler for task: {}", task_payload.task)
        log.debug("Event: ", details=task_payload.model_dump())

    except Exception as e:
        log.error("Error parsing event into TaskPayload: {}", e)
        log.debug("Original event that failed parsing: ", details=event)

        # Return a failure response that Step Functions can handle
        return {
            "flow_control": "failure",
            "error": f"Failed to parse event: {str(e)}",
            "original_event": event,
        }

    try:
        # Load actions from the S3 bucket "{task}.actions"
        log.debug("Loading actions for task: {}", task_payload.task)
        definitions = load_actions(task_payload)
        log.debug("Loaded {} action definitions", len(definitions))

        # Load state - this should have been a document created from "get_facts" for Jinja2 rendering
        log.debug("Loading state for task: {}", task_payload.task)
        context_state = load_state(task_payload)
        log.debug(
            "Loaded state with {} keys",
            len(context_state.keys()) if context_state else 0,
        )

        # Create action helper with loaded definitions and state
        action_helper = Helper(definitions, context_state, task_payload)

        # Execute state machine - designed for Step Functions
        # Instead of a tight loop, do limited iterations
        max_iterations = 10  # Prevent runaway loops
        iteration = 0
        flow_control = FlowControl.from_value(task_payload.flow_control)
        while (
            flow_control == FlowControl.EXECUTE
            and not timeout_imminent(context)
            and iteration < max_iterations
        ):

            iteration += 1
            log.debug("State machine iteration {} (max {})", iteration, max_iterations)

            flow_control = run_state_machine(action_helper, context)
            if isinstance(flow_control, str):
                flow_control = FlowControl.from_value(flow_control)

            # Pause briefly to allow other processes to run
            time.sleep(0.5)

        if flow_control == FlowControl.EXECUTE:
            # Check if we hit the iteration limit
            if iteration >= max_iterations:
                log.warning(
                    "Reached maximum iterations ({}), returning 'execute' for Step Functions retry",
                    max_iterations,
                )

            if timeout_imminent(context):
                log.warning(
                    "Execution stopped due to timeout, Step Functions will retry"
                )

        # Update the task payload with the final flow control state
        task_payload.flow_control = flow_control.value

        # Save state back to S3
        log.debug("Saving state for task: {}", task_payload.task)
        save_state(task_payload, context_state)

        log.debug(
            "Exiting handler with flow_control state: {}", task_payload.flow_control
        )
        log.debug("Execution completed after {} loops", iteration)

        result = task_payload.model_dump()
        log.trace("Handler result: ", details=result)

        return result

    except Exception as e:
        log.error("Error in handler execution: {}", e)
        log.debug(
            "Task payload at time of error: ",
            details=task_payload.model_dump() if task_payload else {},
        )

        # Ensure we have a valid task_payload for the response
        if task_payload:
            task_payload.flow_control = FlowControl.FAILURE.value
            return task_payload.model_dump()
        else:
            # Fallback if task_payload is somehow None
            return {
                "flow_control": FlowControl.FAILURE.value,
                "error": f"Handler execution failed: {str(e)}",
                "original_event": event,
            }


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
        aws.invoke_lambda(
            arn=util.get_execute_lambda_arn(),
            request_payload=task_payload.model_dump(),
            role=util.get_provisioning_role_arn(),
            invocation_type="Event",  # Use Event for async execution
        )

    log.debug("Handler response: ", details=response)
