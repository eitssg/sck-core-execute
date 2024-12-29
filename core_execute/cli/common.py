"""Common commandline paramters"""

from typing import Any
import io
import os
import importlib
import json
from rich.console import Console
from rich.syntax import Syntax
import darkdetect  # type: ignore
from ruamel.yaml import YAML
from ruamel.yaml.scalarstring import DoubleQuotedScalarString

from core_framework.models import ActionDefinition

console = Console()

# Detect OS theme and select appropriate theme
if darkdetect.isDark():
    theme = "native"  # Example dark theme
else:
    theme = "github"  # Example light theme


def yprint(data: Any, format: str = "yaml", end: str = "\n"):
    console.print(Syntax(data, format, theme=theme), end=end)


def cprint(data: Any, format: str = "text", end: str = "\n"):
    console.print(Syntax(data, format, theme=theme), end=end)


def jprint(data: Any, format: str = "json", end: str = "\n"):
    console.print(Syntax(data, format, theme=theme), end=end)


def add_common_parameters(parser):
    """
    Add the common parameters to the parser

    Args:
        parser (subparser | parser): The parser to add the args to
    """
    portfolio = os.getenv("PORTFOLIO")
    app = os.getenv("APP")
    branch = os.getenv("BRANCH")
    build = os.getenv("BUILD")

    parser.add_argument(
        "-p",
        "--portfolio",
        dest="portfolio",
        metavar="<portfolio>",
        type=str,
        help="The portfolio name. Default is the PORTFOLIO environment variable",
        required=True,
        default=portfolio,
    )
    parser.add_argument(
        "-a",
        "--app",
        dest="app",
        metavar="<app>",
        type=str,
        help="The app name. Default is the APP environment variable",
        default=app,
    )
    parser.add_argument(
        "-b",
        "--branch",
        dest="branch",
        metavar="<branch>",
        type=str,
        help="The branch name. Default is the BRANCH environment variable",
        default=branch,
    )
    parser.add_argument(
        "-i",
        "--build",
        dest="build",
        metavar="<build>",
        type=str,
        help="The build name. Default is the BUILD environment variable",
        default=build,
    )


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


def to_yaml(data: dict | list) -> str:
    """Convert data to yaml string."""
    quoted_data = quote_strings(data)

    y = YAML(typ="rt")
    y.default_flow_style = False
    y.indent(mapping=2, sequence=4, offset=2)

    s = io.StringIO()
    y.dump(quoted_data, s)
    return s.getvalue()


def to_json(data: dict | list) -> str:
    """Convert data to json."""
    return json.dumps(data, indent=2)


def read_yaml(stream) -> dict | list:
    """Load the yaml data"""
    yaml = YAML(typ="rt")
    return yaml.load(stream)


def write_yaml(data: dict | list, stream) -> None:
    """Write the yaml data"""

    quoted_actions_list = quote_strings(data)

    y = YAML(typ="rt")
    y.default_flow_style = False
    y.preserve_quotes = True
    y.indent(mapping=2, sequence=4, offset=2)

    y.dump(quoted_actions_list, stream)


def read_json(stream) -> dict | list:
    """Load the json data"""
    return json.load(stream.read())


def write_json(data: dict | list, stream) -> None:
    """Write the json data"""
    json.dump(data, stream, indent=2)


def load_actions_list_from_file(fn: str) -> list[ActionDefinition]:
    """Load the actions list from the file"""

    if not os.path.exists(fn):
        print(f"File not found: {fn}")
        return []

    with open(fn, "r") as f:
        actions_list = read_yaml(f)

    if not isinstance(actions_list, list):
        print(f"Invalid actions list in file: {fn}")
        return []

    result: list[ActionDefinition] = []
    for raw_action in actions_list:
        action = ActionDefinition(**raw_action)
        result.append(action)
    return result


def save_actions_to_file(filename: str, actions_list: list[ActionDefinition]):
    """Save the actions list to the file"""
    data = [ad.model_dump() for ad in actions_list]

    with open(filename, "w") as f:
        write_yaml(data, f)
