"""Provide a function to list the actions for a task"""

import os

import core_framework as util

from .common import (
    add_common_parameters,
    to_yaml,
    load_actions_list_from_file,
    read_yaml,
    cprint,
    yprint,
)


def add_info_subparser(subparsers):
    """Add the info subparser"""

    parser = subparsers.add_parser("info", help="Show information")
    parser.set_defaults(command="info")
    parser.add_argument(
        "--task",
        dest="task",
        metavar="<name",
        type=str,
        help="The task to show information for",
    )
    add_common_parameters(parser)


def run_info(**kwargs) -> dict:

    cprint("Listing actions in the {app_dir}/artefacts/**/{task}.actions file")

    cprint("Generating task payload:\n")

    task_payload = util.generate_task_payload(**kwargs)

    yprint(to_yaml(task_payload.model_dump()))

    app_dir = task_payload.Actions.DataPath
    action_key = task_payload.Actions.Key

    action_file = os.path.join(app_dir, action_key)

    if not os.path.exists(action_file):
        cprint(f"Actions file {action_file} does not exist")
        return {"result": []}

    cprint(f"Current actions file: {action_file}\n")

    deploy_actions = load_actions_list_from_file(action_file)

    yprint(to_yaml([ad.model_dump() for ad in deploy_actions]))

    cprint("There are {} actions in the file\n".format(len(deploy_actions)))

    cprint("Current State Information:\n")

    state_key = task_payload.State.Key
    state_file = os.path.join(app_dir, state_key)

    if not os.path.exists(state_file):
        cprint(f"State file {state_file} does not exist")
        return {"result": deploy_actions}

    with open(state_file, "r") as f:
        state_data = read_yaml(f)

    yprint(to_yaml(state_data))

    cprint("End of information")

    return {"result": deploy_actions}
