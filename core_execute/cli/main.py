"""Profiles the CLI interface called "execute".

Includes helper functions to provide features that allow the user to interact with the Core Automation execute engine
and generate actions to prepare for the execter"""

from typing import Callable
import sys
import gettext
import os
import argparse
import traceback

import core_framework as util

from core_framework.models import TaskPayload

from core_execute import __version__

from core_execute.cli.action import run_action_define, add_action_subparser
from core_execute.cli.info import run_info, add_info_subparser
from core_execute.cli.run import run_action, add_run_subparser
from core_execute.cli.state import run_state, add_state_subparser

from dotenv import load_dotenv
from .common import cprint

load_dotenv()


# Initialize the translation function
def setup_i18n(locale: str = "en"):
    locales_dir = os.path.join(
        os.path.abspath(os.path.dirname(__file__)), "..", "locale"
    )
    gettext.bindtextdomain("messages", locales_dir)
    gettext.textdomain("messages")
    translation = gettext.translation(
        "messages", locales_dir, languages=[locale], fallback=True
    )
    translation.install()
    return translation.gettext


_ = setup_i18n()

COMMAND: dict[str, Callable] = {
    "action": run_action_define,
    "state": run_state,
    "info": run_info,
    "run": run_action,
}


def generate_task_payload(**kwargs) -> TaskPayload:

    task_payload = TaskPayload.from_arguments(**kwargs)

    return task_payload


def parse_args() -> dict:
    """Parse the CLI arguments"""

    client = util.get_client()
    mode = util.get_mode()
    profile = util.get_aws_profile()

    parser = argparse.ArgumentParser(
        description="Execute a Core Automation Action",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="\nSimple Cloud Kit Automation Engine CLI. (c) 2024 EITS\n",
    )

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
        title="Commands", dest="command", metavar="<command>", required=True
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

        cprint(f"\nCore Automation Englne CLI v{__version__}\n")

        cmd = COMMAND.get(args.pop("command"))
        if cmd is not None:
            cmd(**args)

        cprint("\nOperation complete.\n")

    except Exception:
        traceback.print_exc()
        sys.exit(1)


def main():
    """Main entry point for the CLI"""

    execute()


if __name__ == "__main__":
    main()
