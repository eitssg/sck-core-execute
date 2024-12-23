""" Profiles the CLI interface called "execute".

Includes helper functions to provide features that allow the user to interact with the Core Automation execute engine
and generate actions to prepare for the execter """

from typing import Callable
import uuid
import sys
import os
import argparse
import json
import traceback
import time
import importlib
from datetime import datetime, timezone

from ruamel.yaml import YAML
from ruamel.yaml.scalarstring import DoubleQuotedScalarString

import core_logging as log
import core_framework as util

from core_framework.models import TaskPayload
from core_db.registry.client.actions import ClientActions

from core_execute.handler import handler as core_execute_handler
from dotenv import load_dotenv

from core_execute.actionlib.factory import ActionFactory

from core_execute._version import __version__

load_dotenv()

log_stream_name = "core-execute-cli"

log.setup(log_stream_name)


class LambdaExecutionContext(dict):
    """Emulate the lambda execution context object"""

    def __init__(self, max_lambda_time_seconds: int = 300):

        self["function_name"] = f"{log_stream_name}-function"
        self["function_version"] = "$LATEST"
        self["invoked_function_arn"] = (
            f"arn:aws:lambda:us-east-1:123456789012:function:{log_stream_name}-function"
        )
        self["memory_limit_in_mb"] = 128
        self["aws_request_id"] = str(uuid.uuid4())
        self["log_group_name"] = f"/aws/lambda/{log_stream_name}"
        self["log_stream_name"] = log_stream_name

        self.start_time = datetime.now(timezone.utc)
        self.max_lambda_time_seconds = max_lambda_time_seconds
        self.buffer = 10  # 10 second buffer
        self.max_execute_time_seconds = max_lambda_time_seconds - self.buffer

    def get_remaining_time_in_millis(self) -> int:
        """Return the remaining time in milliseconds"""

        elapsed = datetime.now(timezone.utc) - self.start_time
        remaining_time_in_seconds = (
            self.max_lambda_time_seconds - elapsed.total_seconds()
        )
        return int(remaining_time_in_seconds * 1000)

    def timeout_imminent(self) -> bool:
        """Check if the context is about to timeout.

        This function is NOT in a typical lambda context

        """
        elapsed = datetime.now(timezone.utc) - self.start_time
        return elapsed.total_seconds() > self.max_execute_time_seconds


def state_execute(task_playload: TaskPayload) -> TaskPayload:
    """Execute the state"""

    print("Running event: {}...".format(task_playload.Task), end=None)

    event = task_playload.model_dump()

    event = core_execute_handler(event, LambdaExecutionContext())

    # it is expected that the handler will return a TaskPayload dictionary
    # if it does not, this validation will return an error
    task_playload = TaskPayload(**event)

    print("done.  State: {}".format(task_playload.FlowControl))

    return task_playload


def state_wait(task_payload: TaskPayload) -> TaskPayload:

    print("Waiting for 15 seconds...", end=None)

    time.sleep(15)
    task_payload.FlowControl = "execute"

    print("continuing...")

    return task_payload


def state_success(task_payload: TaskPayload) -> TaskPayload:
    print("Executing success state...")

    result = task_payload.model_dump_json()

    with open("simulate-response.json", "w") as f:
        f.write(result)

    print(result)

    return task_payload


def state_failure(task_payload: TaskPayload) -> TaskPayload:

    print("Execution failed...")

    result = task_payload.model_dump_json()

    with open("simulate-response.json", "w") as f:
        f.write(result)

    print(result)

    return task_payload


STATE_MACHINE_FLOW = {
    "execute": state_execute,
    "wait": state_wait,
    "success": state_success,
    "failure": state_failure,
}


def emulate_state_machine(**kwargs):

    task_payload = TaskPayload.from_arguments(**kwargs)
    task_payload.FlowControl = "execute"

    client_vars = ClientActions.get(client=task_payload.DeploymentDetails.Client)

    # bucket_region = client_vars['CLIENT_REGION']
    # bucket_name = '{}{}-core-automation-{}'.format(client_vars.get('SCOPE_PREFIX', ''), client_vars['CLIENT_NAME'], client_vars['CLIENT_REGION'])
    #
    # delivered_by = os.environ["DELIVERED_BY"] if "DELIVERED_BY" in os.environ else "automation"

    while True:

        fc = task_payload.FlowControl
        task_payload = STATE_MACHINE_FLOW[fc](task_payload)
        if (
            task_payload.FlowControl == "success"
            or task_payload.FlowControl == "failure"
        ):
            break

    return {"task_payload": task_payload.model_dump(), "client_vars": client_vars.data}


def action_deploy(**kwargs):
    """Deploy the application"""

    result = emulate_state_machine(**kwargs)
    return {"result": result}


def action_release(**kwargs):
    """Release the application"""

    result = emulate_state_machine(**kwargs)
    return {"result": result}


def action_terdown(**kwargs):
    """Tear down the application"""

    result = emulate_state_machine(**kwargs)
    return {"result": result}


def get_module_name_parts(relative_module_name):
    """Helper function to get module name parts and class name."""
    module_name_parts = relative_module_name.split(".")
    prefix = "::".join([word.upper() for word in module_name_parts[:-1]])
    action_name = "".join(
        [word.capitalize() for word in module_name_parts[-1].split("_")]
    )
    class_name = action_name + "Action"
    path_action_name = prefix + "::" + action_name
    return path_action_name, class_name


def get_module_description(full_module_name):
    """Helper function to get the module description."""
    module = importlib.import_module(full_module_name)
    description = module.__doc__

    return description


def quote_strings(data):
    """Recursively quote all strings in the data."""
    if isinstance(data, dict):
        return {k: v if k == "Label" else quote_strings(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [quote_strings(v) for v in data]
    elif isinstance(data, str):
        return DoubleQuotedScalarString(data)
    else:
        return data


def print_actions_list(actions_list: list[dict], format: str = "yaml"):

    print()

    if format == "json":
        print(json.dumps(actions_list, indent=2))
    elif format == "yaml":
        y = YAML(typ="rt")
        y.default_flow_style = False
        y.preserve_quotes = True
        quoted_actions_list = quote_strings(actions_list)
        y.dump(quoted_actions_list, sys.stdout)
    else:
        print("Invalid format.  Use 'yaml' or 'json'.")


def save_actions(filename: str, actions_list: list[dict], format: str = "yaml"):
    """Save the actions to a YAML file"""

    if filename:
        with open(filename, "w") as f:
            if format == "json":
                json.dump(actions_list, f, indent=2)
            elif format == "yaml":
                y = YAML(typ="rt")
                y.default_flow_style = False
                y.preserve_quotes = True
                quoted_actions_list = quote_strings(actions_list)
                y.dump(quoted_actions_list, f)
        print(f"Actions saved to {filename}")


def action_template(**kwargs):

    action_name = kwargs.get("action")
    print("Retrieving template for action: {}".format(action_name))

    try:
        module_path, class_name = ActionFactory.get_module_and_class_name(action_name)

        # Import the module and instantiate the action class
        module = importlib.import_module(module_path)
        get_template_for_paramters = getattr(module, "generate_template")

        if get_template_for_paramters is None:
            raise Exception("No generate_template function in module")

        template = get_template_for_paramters()

        # Temporary hardcoding of parameters for my own testing
        template.Label = f"action-{action_name.lower().replace("::","-")}-label"
        template.Params.Account = "154798051514"
        template.Params.Region = "ap-southeast-1"
        template.Scope = "build"

        actions_list = [template.model_dump()]

        save_actions(kwargs.get("filename", None), actions_list, format=kwargs.get("format", "yaml"))

        if kwargs.get("out"):
            print_actions_list(actions_list, format=kwargs.get("format", "yaml"))

        return {"actions": actions_list}

    except Exception as e:
        print(e)
        print(
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

    print("Actions list:")

    # In the core_execute module, there is a filer called actionlib/actions/* that contains
    # a library of classes that extend core_execute.actionlib.action.BaseAction and I wish
    # to iterate through all of the modules where the python file is s snake-case and the
    # class name is a camel-case version of the file name ending with "Action" and once
    # the class is instantiated, I want to return a dictionary with the action name and the action"

    module_base = "core_execute.actionlib.actions"
    module_path = os.path.join(
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..")),
        module_base.replace(".", os.path.sep),
    )

    print("\nLooking in {}".format(module_base))

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

    print("Available actions:")
    for k, v in module_names.items():
        print(f"    {k:35}", v[2])

    return {"result": module_names}


def action_create(**kwargs):
    """Create the action"""

    print("Create the actions in the {app_dir}/artefacts/**/{task}.actions file")

    result = kwargs

    return {"result": result}


def action_deleate(**kwargs):

    result = kwargs

    return {"result": result}


def run_action_define(**kwargs) -> dict:
    """Define the action"""

    ACTION_COMMAND = {
        "list": action_list,
        "create": action_create,
        "delete": action_deleate,
        "template": action_template,
    }
    command = kwargs.pop("subcommand")
    if command in ACTION_COMMAND:
        result = ACTION_COMMAND[command](**kwargs)
    else:
        result = {"error": "No ACTION_COMMAND action"}

    return {"result": result}


def run_action(**kwargs) -> dict:
    """Run the action"""

    RUN_ACTIONS = {
        "deploy": action_deploy,
        "release": action_release,
        "teardown": action_terdown,
    }

    action = kwargs.get("task")
    if action in RUN_ACTIONS:
        result = RUN_ACTIONS[action](**kwargs)
    else:
        result = {"error": "No RUN_ACTIONS action"}
    return {"result": result}


def run_state(**kwargs) -> dict:

    state: dict = {}

    return {"result": "success", "state": state}


def run_info(**kwargs) -> dict:

    if "task" not in kwargs:
        kwargs["task"] = "info"

    info: dict = kwargs

    task_payload = util.generate_task_payload(**kwargs)

    return {
        "result": "success",
        "input parameters": info,
        "task_payload": task_payload.model_dump(),
        "app_data": {
            "app_path": task_payload.Package.AppPath,
            "temp_dir": task_payload.Package.TempDir,
        },
    }


COMMAND: dict[str, Callable] = {
    "action": run_action_define,
    "state": run_state,
    "info": run_info,
    "run": run_action,
}


def add_common_parameters(parser):

    portfolio = os.getenv("PORTFOLIO")
    app = os.getenv("APP")
    branch = os.getenv("BRANCH")
    build = os.getenv("BUILD")

    parser.add_argument(
        "-p, --portfolio",
        dest="portfolio",
        metavar="<portfolio>",
        type=str,
        help="The portfolio name. Default is the PORTFOLIO environment variable",
        required=True,
        default=portfolio,
    )
    parser.add_argument(
        "-a, --app",
        dest="app",
        metavar="<app>",
        type=str,
        help="The app name. Default is the APP environment variable",
        default=app,
    )
    parser.add_argument(
        "-b, --branch",
        dest="branch",
        metavar="<branch>",
        type=str,
        help="The branch name. Default is the BRANCH environment variable",
        default=branch,
    )
    parser.add_argument(
        "-i, --build",
        dest="build",
        metavar="<build>",
        type=str,
        help="The build name. Default is the BUILD environment variable",
        default=build,
    )


def add_action_subparser(subparsers):
    """Add the action subparser"""

    parser = subparsers.add_parser("action", help="Manage the {task}.actions")
    parser.set_defaults(command="action")

    sp = parser.add_subparsers(
        title="action",
        dest="subcommand",
        metavar="command",
        required=True,
        help="Action subcommand: list, create, delete",
    )

    p = sp.add_parser("list", help="List the actions")
    p.set_defaults(subcommand="list")

    p = sp.add_parser("create", help="Create the action")
    p.set_defaults(subcommand="create")

    p = sp.add_parser("delete", help="Delete the action")
    p.set_defaults(subcommand="delete")

    p = sp.add_parser("template", help="Get's a template from the action")
    p.set_defaults(subcommand="template")
    p.add_argument(
        "-a, --action",
        dest="action",
        metavar="<action>",
        type=str,
        help="The name of the action.  See 'core-execute action list'",
    )
    p.add_argument(
        "-f, --filename",
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
    p.add_argument("--out", action="store_true", help="Print the template to stdout")


def add_state_subparser(subparsers):
    """Add the state subparser"""

    parser = subparsers.add_parser("state", help="Manage the {task}.state")
    parser.set_defaults(command="state")

    parser.add_argument(
        "operation",
        type=str,
        choices=["generate", "save", "delete"],
        help="Actions to perform on the state name",
    )

    parser.add_argument(
        "task", type=str, help="The task that the state is associated with"
    )
    parser.add_argument(
        "--filename", type=str, help="The filename to save the state information"
    )

    add_common_parameters(parser)


def add_run_subparser(subparsers):
    """Add the run subparser"""

    parser = subparsers.add_parser(
        "run", help="Run the {task}.  One of [deploy, release, teardown]"
    )
    parser.set_defaults(command="run")

    parser.add_argument(
        "task", type=str, help="Run a command [deploy, release, teardown]"
    )

    add_common_parameters(parser)


def add_info_subparser(subparsers):
    """Add the info subparser"""

    parser = subparsers.add_parser("info", help="Show information")
    parser.set_defaults(command="info")

    add_common_parameters(parser)


def generate_task_payload(**kwargs) -> TaskPayload:

    task_payload = TaskPayload.from_arguments(**kwargs)

    return task_payload


def parse_args() -> dict:
    """Parse the CLI arguments"""

    client = util.get_client()
    mode = util.get_mode()
    profile = util.get_aws_profile()

    parser = argparse.ArgumentParser(description="Execute a Core Automation Action")

    parser.add_argument(
        "--client",
        dest="client",
        type=str,
        metavar="<client>",
        help=f"The client name. Default is '{client}'",
        required=client is None,
        default=client,
    )
    parser.add_argument(
        "--mode",
        dest="mode",
        type=str,
        metavar="<mode>",
        help=f"The execution mode. Default is '{mode}'",
        default=mode,
    )
    parser.add_argument(
        "--aws-profile",
        dest="profile",
        type=str,
        metavar="<profile>",
        help=f"AWS Profile to use. Default is '{profile}'",
        default=profile,
    )

    subparsers = parser.add_subparsers(
        title="commands", dest="command", metavar="command", required=True
    )

    add_action_subparser(subparsers)
    add_state_subparser(subparsers)
    add_info_subparser(subparsers)
    add_run_subparser(subparsers)

    data = vars(parser.parse_args())

    return data


def execute():
    """Execute the CLI"""

    try:
        args = parse_args()

        print(f"Core Automation Englne CLI v{__version__}\n")

        cmd = COMMAND.get(args.pop("command"))
        if cmd is not None:
            cmd(**args)

        print("\nOperation complete.")

    except Exception:
        traceback.print_exc()
        sys.exit(1)


def main():
    """Main entry point for the CLI"""

    execute()


if __name__ == "__main__":
    main()
