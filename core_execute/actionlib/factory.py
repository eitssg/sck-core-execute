"""Factory module for creating action instances from action definitions."""

from typing import Any

import importlib
import re

from core_framework.models import ActionDefinition
from core_execute.actionlib.action import BaseAction

from core_framework.models import DeploymentDetails


class ActionFactory:
    """Action factory class"""

    actions_path = ["core_execute", "actionlib", "actions"]
    ACTION_CLASS_NAME_SUFFIX: str = "Action"

    @staticmethod
    def __camel_to_snake_case(string: str) -> str:
        # Separate capitals after a non-capital with an underscore (ignores special characters)
        return re.sub("([a-z0-9])([A-Z])", r"\1_\2", string).lower()

    @staticmethod
    def get_module_and_class_name(action_type: str) -> tuple[str, str]:
        """Get the module name from the action type"""

        # Work out the class name and module path from the action type
        split_type = action_type.split("::")
        class_name = split_type[-1] + ActionFactory.ACTION_CLASS_NAME_SUFFIX
        module_path = ActionFactory.__camel_to_snake_case(
            ".".join(ActionFactory.actions_path + split_type)
        )
        return module_path, class_name

    @staticmethod
    def load(
        definition: ActionDefinition,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ) -> BaseAction:

        # Don't allow relative references
        if ".." in definition.Type:
            raise NotImplementedError("Unknown action '{}'".format(definition.Type))

        module_path, class_name = ActionFactory.get_module_and_class_name(
            definition.Type
        )

        # Import the module and instantiate the action class
        try:

            module = importlib.import_module(module_path)
            klass = getattr(module, class_name)
            return klass(definition, context, deployment_details)

        except (ImportError, AttributeError):
            raise NotImplementedError("Unknown action '{}'".format(definition.Type))

        except Exception as e:
            raise RuntimeError(
                "Cannot instantiate class '{}'".format(class_name)
            ) from e
