"""Module to provide functions to run actions via the command line"""

import core_framework as util

from ..stepfn import emulate_state_machine, generate_execution_name

from .common import add_common_parameters


def action_deploy(**kwargs):
    """Deploy the application"""

    task_payload = util.generate_task_payload(**kwargs)
    name = generate_execution_name(task_payload)

    result = emulate_state_machine(name, task_payload)

    return {"result": result}


def action_release(**kwargs):
    """Release the application"""

    task_payload = util.generate_task_payload(**kwargs)
    name = generate_execution_name(task_payload)

    result = emulate_state_machine(name, task_payload)

    return {"result": result}


def action_terdown(**kwargs):
    """Tear down the application"""

    task_payload = util.generate_task_payload(**kwargs)
    name = generate_execution_name(task_payload)

    result = emulate_state_machine(name, task_payload)

    return {"result": result}


RUN_ACTIONS = {
    "deploy": action_deploy,
    "release": action_release,
    "teardown": action_terdown,
}


def run_action(**kwargs) -> dict:
    """Run the action"""

    action = kwargs.get("task")
    if action in RUN_ACTIONS:
        result = RUN_ACTIONS[action](**kwargs)
    else:
        result = {"error": "No RUN_ACTIONS action"}
    return {"result": result}


def add_run_subparser(subparsers):
    """Add the run subparser to the subparsers

    Args:
        subparsers (subparsers): The subparsers to add the run subparser to

    """

    parser = subparsers.add_parser(
        "run", help="Run the {task}.  One of [deploy, release, teardown]"
    )
    parser.set_defaults(command="run")

    parser.add_argument(
        "task",
        type=str,
        choices=RUN_ACTIONS.keys(),
        help="Run a command [deploy, release, teardown]",
    )

    add_common_parameters(parser)
