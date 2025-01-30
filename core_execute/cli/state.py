"""Module to assist with managing the state of a task action"""

import os

import core_framework as util
from core_framework.models import TaskPayload
from core_framework import generate_task_payload

from .common import (
    add_common_parameters,
    yprint,
    cprint,
)


def add_state_subparser(subparsers):
    """Add the state subparser

    Args:
        subparsers (subparsers): The subparsers to add the state subparser to

    """

    parser = subparsers.add_parser("state", help="Manage the {task}.state")
    parser.set_defaults(command="state")

    sps = parser.add_subparsers(
        title="Operation",
        dest="operation",
        metavar="command",
        required=True,
        help="State subcommand: generate, save, delete",
    )

    generate_parser = sps.add_parser("generate", help="Generate a new state")
    add_common_parameters(generate_parser)
    generate_parser.add_argument(
        "--out",
        dest="filename",
        metavar="<filename>",
        type=str,
        help="The filename to save the task.actions template",
        required=False,
    )
    generate_parser.add_argument(
        "--format",
        choices=["json", "yaml"],
        type=str,
        help="The format to save the template: yaml or json",
        required=False,
        default="yaml",
    )

    save_parser = sps.add_parser("save", help="Save the state to a file")
    save_parser.add_argument(
        "--task",
        dest="task",
        metavar="<name>",
        type=str,
        help="The task that the state is associated with",
        required=True,
    )
    add_common_parameters(save_parser)
    save_parser.add_argument(
        "--in",
        dest="filename",
        metavar="<filename>",
        type=str,
        help="The filename to save the task.actions template",
        required=False,
    )
    save_parser.add_argument(
        "--format",
        choices=["json", "yaml"],
        type=str,
        help="The format to save the state to: yaml or json",
        required=False,
        default="yaml",
    )

    delete_parser = sps.add_parser("delete", help="Delete the state")
    delete_parser.add_argument(
        "--task",
        dest="task",
        metavar="<name>",
        type=str,
        help="The task that the state is associated with",
    )
    add_common_parameters(delete_parser)


def generate_state(**kwargs) -> dict:
    """Generate a new state for the task"""

    cprint("Generated state template:")

    fn = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "data", "state_template.yaml"
    )
    with open(fn, "r") as f:
        state = util.read_yaml(f)

    if not isinstance(state, dict):
        cprint("Error: Invalid state template")
        return {}

    yprint(util.to_yaml(state))

    filename = kwargs.get("filename")
    if filename:
        cprint("Saving to file: {}".format(filename))

        response = "y"
        if os.path.exists(filename):
            while True:
                response = input(
                    "File already exists.  Do you wish to overwrite? [y/n]: "
                )
                response = response.strip().lower()
                if response not in ["y", "n"]:
                    cprint("Invalid response.  Either 'y' or 'n'")
                    continue
                if response == "y":
                    cprint("Overwriting file...", end="")
                else:
                    cprint("Exiting without saving")
                    return state
                break

        with open(filename, "w") as f:
            util.write_yaml(state, f)

        cprint("file saved: {}.".format(filename))

    return state


def __get_artefact_path(**kwargs) -> str | None:
    """Get the artefact path"""

    task_payload: TaskPayload = generate_task_payload(**kwargs)

    if not task_payload.Task:
        cprint("Exiting. Cannot determine task!")
        return None

    data_path = task_payload.State.DataPath
    app_key = task_payload.State.Key

    cprint("")
    yprint(util.to_yaml(task_payload.model_dump()))

    artefact_path = os.path.join(data_path, app_key)

    return artefact_path


def __get_state(**kwargs) -> dict:

    filename = kwargs.get("filename")
    if not filename:
        cprint("No filename provided.  Exiting without state.")
        return {}

    format = kwargs.get("format", "yaml")

    # load the input filename depending on the format
    cprint("Loading state from file: {}\n".format(filename))

    with open(filename, "r") as f:
        if format == "json":
            state = util.read_json(f)
        else:
            state = util.read_yaml(f)

    if not isinstance(state, dict):
        cprint("Error: Invalid state file")
        return {}

    yprint(util.to_yaml(state))

    return state


def save_state(**kwargs):
    """Save the state to a file"""

    state = __get_state(**kwargs)

    if not state:
        cprint("Exiting without saving.  Cannot determine state!")
        return state

    artefact_path = __get_artefact_path(**kwargs)

    if not artefact_path:
        cprint("Exiting without saving.  Cannot determine artefact path!")
        return state

    cprint("Artefact Path: {}\n".format(artefact_path))
    while True:
        response = input("Is this the artefact you wish to save the state to? [y/n]: ")
        response = response.strip().lower()
        if response not in ["y", "n"]:
            cprint("Invalid response.  Either 'y' or 'n'")
            continue
        if response == "y":
            cprint("Saving state to file...", end="")
        else:
            cprint("Exiting without saving")
            return state
        break

    dirname = os.path.dirname(artefact_path)

    os.makedirs(dirname, exist_ok=True)

    with open(artefact_path, "w") as f:
        util.write_yaml(state, f)

    cprint("State saved to file: {}".format(artefact_path))

    return state


def delete_state(**kwargs):
    """Delete the state"""

    artefact_path = __get_artefact_path(**kwargs)

    if not artefact_path:
        cprint("Exiting without deleting.  Cannot determine artefact path")
        return

    cprint("Artefact Path: {}\n".format(artefact_path))

    while True:
        response = input("Is this the artefact you wish to delete? [y/n]: ")
        response = response.strip().lower()
        if response not in ["y", "n"]:
            cprint("Invalid response.  Either 'y' or 'n'")
            continue
        if response == "y":
            cprint("Deleting state to file...", end="")
        else:
            cprint("Exiting without deleting")
            return
        break

    if os.path.exists(artefact_path):
        os.remove(artefact_path)
        cprint("State deleted: {}".format(artefact_path))
    else:
        cprint("State does not exist: {}".format(artefact_path))

    return


OPERATIONS = {
    "generate": generate_state,
    "save": save_state,
    "delete": delete_state,
}


def run_state(**kwargs) -> dict:

    state: dict = {}

    operation = kwargs.get("operation")

    cprint(f"Running state operation: {operation}")

    if operation in OPERATIONS:
        state = OPERATIONS[operation](**kwargs)

    return {"result": "success", "state": state}
