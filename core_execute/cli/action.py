import os
import importlib

from core_framework import generate_task_payload
from core_framework.models import ActionDefinition

from ..actionlib.factory import ActionFactory

from .common import (
    to_yaml,
    write_yaml,
    to_json,
    write_json,
    add_common_parameters,
    get_module_name_parts,
    get_module_description,
    load_actions_list_from_file,
    save_actions_to_file,
    cprint,
    yprint,
    jprint,
)


def add_action_subparser(subparsers):
    """Add the action subparser

    Args:
        subparsers (subparsers): The subparsers to add the action subparser to

    """

    parser = subparsers.add_parser("action", help="Manage the {task}.actions")
    parser.set_defaults(command="action")

    sp = parser.add_subparsers(
        title="Action",
        dest="subcommand",
        metavar="command",
        required=True,
        help="Action subcommand: list, add, delete, template",
    )

    p = sp.add_parser("list", help="List the actions available")
    p.set_defaults(subcommand="list")

    p = sp.add_parser("add", help="Add an action to the {task}.actions")
    p.set_defaults(subcommand="add")
    p.add_argument(
        "--task",
        dest="task",
        metavar="<name>",
        type=str,
        help="The task name to add the action to",
        required=True,
    )
    add_common_parameters(p)
    p.add_argument(
        "--in",
        dest="filename",
        metavar="<filename>",
        type=str,
        help="The input file containing the actions to add to the task",
        required=True,
    )

    p = sp.add_parser("delete", help="Delete the action")
    p.set_defaults(subcommand="delete")
    p.add_argument(
        "--task",
        dest="task",
        metavar="<name>",
        type=str,
        help="The task name to delete the action from",
    )
    p.add_argument(
        "--label",
        dest="label",
        metavar="<label>",
        type=str,
        help="The label of the action to delete",
    )
    add_common_parameters(p)

    p = sp.add_parser("template", help="Get's a template from the action")
    p.set_defaults(subcommand="template")
    p.add_argument(
        "-a",
        "--action",
        dest="action",
        metavar="<action>",
        type=str,
        help="The name of the action.  See 'core-execute action list'",
        required=True,
    )
    p.add_argument(
        "--out",
        dest="filename",
        metavar="<filename>",
        type=str,
        help="The filename to save the task.actions template",
        required=False,
    )
    p.add_argument(
        "--format",
        choices=["json", "yaml"],
        type=str,
        help="The format to save the template: yaml or json",
        required=False,
        default="yaml",
    )


def print_actions_list(actions_list: list[dict], format: str = "yaml"):

    cprint("")

    if format == "json":
        jprint(to_json(actions_list))
    elif format == "yaml":
        yprint(to_yaml(actions_list))
    else:
        cprint("Invalid format.  Use 'yaml' or 'json'.")


def save_actions(filename: str, actions_list: list[dict], format: str = "yaml"):
    """Save the actions to a YAML file"""

    if filename:
        with open(filename, "w") as f:
            if format == "json":
                write_json(actions_list, f)
            elif format == "yaml":
                write_yaml(actions_list, f)
        cprint(f"Actions saved to: {filename}")


def action_template(**kwargs):

    action_name = kwargs.get("action")
    cprint("Retrieving template for action: {}".format(action_name))

    try:
        module_path, class_name = ActionFactory.get_module_and_class_name(action_name)

        # Import the module and instantiate the action class
        module = importlib.import_module(module_path)
        get_template_for_paramters = getattr(module, "generate_template")

        if get_template_for_paramters is None:
            raise Exception("No generate_template function in module")

        template = get_template_for_paramters()

        # Temporary hardcoding of parameters for my own testing
        template.Label = f"action-{action_name.lower().replace("::", "-")}-label"
        template.Params.Account = "154798051514"
        template.Params.Region = "ap-southeast-1"
        template.Scope = "build"

        actions_list = [template.model_dump()]

        cprint("")

        yprint(to_yaml(actions_list))

        filename = kwargs.get("filename", None)
        if filename:
            save_actions(
                kwargs.get("filename", None),
                actions_list,
                format=kwargs.get("format", "yaml"),
            )
        else:
            cprint("Use --out <filename> to save the template to a file")

    except Exception as e:
        cprint(e)
        cprint(
            "Cannot load action: {}.  Use 'core-execute action list' to get a lisst of actions".format(
                action_name
            )
        )
        return {
            "tempalate": {
                "action": action_name,
                "error": str(e),
                "info": "Get a list of actions with 'core-execute action list'",
            }
        }


def action_list(**kwargs):
    """List the actions"""

    cprint("Actions list:")

    # In the core_execute module, there is a filer called actionlib/actions/* that contains
    # a library of classes that extend core_execute.actionlib.action.BaseAction and I wish
    # to iterate through all of the modules where the python file is s snake-case and the
    # class name is a camel-case version of the file name ending with "Action" and once
    # the class is instantiated, I want to return a dictionary with the action name and the action"

    module_base = "core_execute.actionlib.actions"
    module_path = os.path.join(
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")),
        module_base.replace(".", os.path.sep),
    )

    cprint("\nLooking in {}".format(module_base))

    # Walk the directory and generate a list of module names that can be used with pytin importlib
    # that consists of the full module path of directories separated by "." and the file name without
    # the .py extension separated by "." so I can use importlib(fn) to import the module

    module_names = {}

    for root, dirs, files in os.walk(module_path):
        for file in files:
            if file.endswith(".py") and file not in ["__init__.py", "_TEMPLATE.py"]:
                # Construct the module path
                relative_path = os.path.relpath(os.path.join(root, file), module_path)
                module_name = os.path.splitext(relative_path.replace(os.path.sep, "."))[
                    0
                ]
                full_module_name = f"{module_base}.{module_name}"

                action_name, class_name = get_module_name_parts(module_name)

                description = get_module_description(full_module_name)

                module_names[action_name] = (full_module_name, class_name, description)

    cprint("Available actions:")
    for k, v in module_names.items():
        cprint(f"    {k:35}", v[2])

    return {"result": module_names}


def __add_action_to_list(action_list: list[ActionDefinition], action: ActionDefinition):
    """Add an action to the list"""

    for i in range(len(action_list)):
        if action_list[i].Label == action.Label:
            action_list[i] = action
            return action_list
    action_list.append(action)

    return action_list


def action_add(**kwargs):
    """Create the action"""

    cprint("Add the actions in the {app_dir}/artefacts/**/{task}.actions file")

    cprint("Generating task payload:\n")

    task_payload = generate_task_payload(**kwargs)

    yprint(to_yaml(task_payload.model_dump()))

    fn = kwargs.get("filename")
    if not fn:
        cprint("No filename specified.  Exiting.")
        return {"error": "No filename specified"}

    action_defs = load_actions_list_from_file(fn)

    print("Actions list to add to task '{}':\n".format(task_payload.Task))

    yprint(to_yaml([ad.model_dump() for ad in action_defs]))

    app_dir = task_payload.Actions.AppPath
    action_key = task_payload.Actions.Key

    action_file = os.path.join(app_dir, action_key)

    cprint(f"Add actions to action file: {action_file}\n")

    while True:
        response = input("Is this the correct action file?  [y/n]: ")
        response = response.strip().lower()
        if response not in ["y", "n"]:
            cprint("Invalid response.  Either 'y' or 'n'")
            continue
        if response == "n":
            cprint("Exiting without saving")
            return {"error": "User aborted"}
        break

    cprint("\nSaving...", end="")

    deploy_actions = load_actions_list_from_file(action_file)

    for ad in action_defs:
        deploy_actions = __add_action_to_list(deploy_actions, ad)

    save_actions_to_file(action_file, deploy_actions)

    cprint("Actions saved to: {}\n".format(action_file))

    yprint(to_yaml([ad.model_dump() for ad in deploy_actions]))

    cprint("There are now {} actions in the file".format(len(deploy_actions)))

    return {"result": action_defs}


def __label_is_in_actions_list(label: str, actions_list: list[ActionDefinition]) -> bool:
    """Check if the label is in the actions list"""

    for action in actions_list:
        if action.Label == label:
            return True

    return False


def action_delete(**kwargs):

    cprint("Deleate an action in the {app_dir}/artefacts/**/{task}.actions file")

    cprint("Generating task payload:\n")

    task_payload = generate_task_payload(**kwargs)

    yprint(to_yaml(task_payload.model_dump()))

    label = kwargs.get("label")

    if not label:
        cprint("No label specified.  Exiting.")
        return {"error": "No label specified"}

    app_dir = task_payload.Actions.AppPath
    action_key = task_payload.Actions.Key

    action_file = os.path.join(app_dir, action_key)

    cprint(f"Current actions file: {action_file}\n")

    deploy_actions = load_actions_list_from_file(action_file)

    yprint(to_yaml([ad.model_dump() for ad in deploy_actions]))

    if not __label_is_in_actions_list(label, deploy_actions):
        cprint("Label '{}' not in the actions list".format(label))
        return {"error": "Label not in actions list"}

    cprint("The action with the label '{}' will be deleted.\n".format(label))

    while True:
        response = input("Is this the correct action file?  [y/n]: ")
        response = response.strip().lower()
        if response not in ["y", "n"]:
            cprint("Invalid response.  Either 'y' or 'n'")
            continue
        if response == "n":
            cprint("Exiting without saving")
            return {"error": "User aborted"}
        break

    cprint("\nDeleting action...", end="")

    deploy_actions = [ad for ad in deploy_actions if ad.Label != label]

    save_actions_to_file(action_file, deploy_actions)

    cprint("Actions saved to: {}\n".format(action_file))

    yprint(to_yaml([ad.model_dump() for ad in deploy_actions]))

    cprint("There are now {} actions in the file".format(len(deploy_actions)))

    return {"result": deploy_actions}


ACTION_COMMAND = {
    "list": action_list,
    "add": action_add,
    "delete": action_delete,
    "template": action_template,
}


def run_action_define(**kwargs) -> dict:
    """Define the action"""

    command = kwargs.pop("subcommand")
    if command in ACTION_COMMAND:
        result = ACTION_COMMAND[command](**kwargs)
    else:
        result = {"error": "No ACTION_COMMAND action"}

    return {"result": result}
