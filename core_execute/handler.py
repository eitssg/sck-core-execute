from typing import Any

import core_logging as log

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
    Recieve an "Actions" event request and run it!

    Args:
        event (dict): The "Lambda Event" from the requester.
        context (dict): lambda context (Ex: cognito, SQS, SNS, etc). This is where you can get, for example,
                        the lambda runtime lifetime, memory, etc. so you know how long the lambda can run.
                        This is helpful if you have long-running actions and the lambda function will terminate.

                        Better use step functions when running long-running actions.
    """
    try:
        # Task payload is a model object should have been crated with TaskPayload.model_dump()
        task_payload = TaskPayload(**event)

        # If the user has not passed in a flow_control, set it to "execute"
        # The aws step-function will set this to "execute" (or whatever) when it calls this method during the execution.
        if not task_payload.flow_control:
            task_payload.flow_control = "execute"

    except Exception as e:
        log.error("Error parsing event: {}", e)
        raise

    try:
        log.setup(task_payload.identity)

        log.trace("Entering core_execute.handler")

        log.info("Entering handler event: {}", task_payload.task)

        log.debug("Event: ", details=task_payload.model_dump())

        # Load actions from the s3 bucket "{task}.actions"
        definitions = load_actions(task_payload)

        # Load state.  This should have been a document created from "get_facts" for Jinja2 rendering
        context_state = load_state(task_payload)
        # context_state["name"] = "initial_state"

        action_helper = Helper(definitions, context_state, task_payload)

        # Run the execution state machine ( but we can only stay rnning for X minutes (see deployment spec))
        while task_payload.flow_control == "execute" and not timeout_imminent(context):
            task_payload.flow_control = run_state_machine(action_helper, context)

        # Save state
        save_state(task_payload, context_state)

        log.debug("Exiting (FlowControl with state = {})", task_payload.flow_control)

        result = task_payload.model_dump()

        log.debug("Execution Complete.")

        log.trace("Result: ", details=result)

        return result

    except Exception as e:
        log.error("Error in handler: {}", e)

        task_payload.flow_control = "failure"
        return task_payload.model_dump()
