""" Profiles the CLI interface called "execute".

Includes helper functions to provide features that allow the user to interact with the Core Automation execute engine
and generate actions to prepare for the execter """

from typing import Callable
import sys
import os
import argparse
import json
import traceback

import core_framework as util

from core_framework.models import TaskPayload
from core_db.response import Response
from core_db.registry.client.actions import ClientActions

from core_execute.handler import handler
from dotenv import load_dotenv

load_dotenv()


def emulate_state_machine(**kwargs):

    task_payload = TaskPayload.from_arguments(**kwargs)

    client_vars = ClientActions.get(
        client=task_payload.DeploymentDetails.Client
    )

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


RUN_ACTIONS = {
    "deploy": action_deploy,
    "release": action_release,
    "terdown": action_terdown,
}


def run_action(**kwargs) -> dict:
    """Run the action"""

    action = kwargs.get("task")
    if action in RUN_ACTIONS:
        result = RUN_ACTIONS[action](**kwargs)

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
    "action": run_action,
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

    parser.add_argument(
        "subcommand",
        choices=["list", "create", "delete"],
        type=str,
        metavar="command",
        help="The action command",
    )

    add_common_parameters(parser)


def add_state_subparser(subparsers):
    """Add the state subparser"""

    parser = subparsers.add_parser("state", help="Manage the {task}.state")
    parser.set_defaults(command="state")

    parser.add_argument("name", type=str, help="The state name")
    parser.add_argument("args", type=str, help="The state arguments")

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

        cmd = COMMAND.get(args.pop("command"))
        if cmd is not None:
            result = cmd(**args)

        print(json.dumps(result, indent=2))

    except Exception as e:
        print(e)
        #traceback.print_exc()
        sys.exit(1)


def main():
    """Main entry point for the CLI"""

    execute()


if __name__ == "__main__":
    main()
