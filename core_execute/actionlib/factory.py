"""Factory module for creating action instances from action definitions."""

from typing import Any

import importlib
import re

import core_logging as log

from core_framework.models import ActionSpec
from pydantic import ValidationError
from core_execute.actionlib.action import BaseAction

from core_framework.models import DeploymentDetails

from .actions.aws.kms.create_grants import CreateGrantsAction
from .actions.aws.rds.modify_db_instance import ModifyDbInstanceAction
from .actions.aws.copy_image import CopyImageAction
from .actions.aws.create_cloud_front_invalidation import (
    CreateCloudFrontInvalidationAction,
)
from .actions.aws.create_image import CreateImageAction
from .actions.aws.create_stack import CreateStackAction
from .actions.aws.delete_ecr_repository import DeleteEcrRepositoryAction
from .actions.aws.delete_image import DeleteImageAction
from .actions.aws.delete_security_group_enis import DeleteSecurityGroupEnisAction
from .actions.aws.delete_stack import DeleteStackAction
from .actions.aws.delete_user import DeleteUserAction
from .actions.aws.duplicate_image_to_account import DuplicateImageToAccountAction
from .actions.aws.empty_bucket import EmptyBucketAction
from .actions.aws.get_stack_outputs import GetStackOutputsAction
from .actions.aws.get_stack_references import GetStackReferencesAction
from .actions.aws.put_event import PutEventAction
from .actions.aws.put_metric_data import PutMetricDataAction
from .actions.aws.share_image import ShareImageAction
from .actions.aws.unprotect_elb import UnprotectELBAction
from .actions.aws.upload_context import UploadContextAction
from .actions.system.no_op import NoOpAction
from .actions.system.set_variables import SetVariablesAction


# There is an argument that suggests that each and every one of these should be a lambda function

valid_actions = [
    ("AWS::KMS::CreateGrants", CreateGrantsAction),
    ("AWS::RDS::ModifyDbInstance", ModifyDbInstanceAction),
    ("AWS::CopyImage", CopyImageAction),
    ("AWS::CreateCloudFrontInvalidation", CreateCloudFrontInvalidationAction),
    ("AWS::CreateImage", CreateImageAction),
    ("AWS::CreateStack", CreateStackAction),
    ("AWS::CreateStackSet", NoOpAction),
    ("AWS::DeleteEcrRepository", DeleteEcrRepositoryAction),
    ("AWS::DeleteImage", DeleteImageAction),
    ("AWS::DeleteSecurityGroupEnis", DeleteSecurityGroupEnisAction),
    ("AWS::DeleteStack", DeleteStackAction),
    ("AWS::DeleteStackSet", NoOpAction),
    ("AWS::DeleteUser", DeleteUserAction),
    ("AWS::DuplicateImageToAccount", DuplicateImageToAccountAction),
    ("AWS::EmptyBucket", EmptyBucketAction),
    ("AWS::GetStackOutputs", GetStackOutputsAction),
    ("AWS::GetStackReferences", GetStackReferencesAction),
    ("AWS::PutEvent", PutEventAction),
    ("AWS::PutMetricData", PutMetricDataAction),
    ("AWS::ShareImage", ShareImageAction),
    ("AWS::UnprotectElb", UnprotectELBAction),
    ("AWS::UploadContext", UploadContextAction),
    ("SYSTEM::NoOp", NoOpAction),
    ("SYSTEM::SetVariables", SetVariablesAction),
]


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
        """Get the module name from the action kind"""

        # Work out the class name and module path from the action kind
        split_type = action_type.split("::")
        class_name = split_type[-1] + ActionFactory.ACTION_CLASS_NAME_SUFFIX
        module_path = ActionFactory.__camel_to_snake_case(
            ".".join(ActionFactory.actions_path + split_type)
        )
        return module_path, class_name

    @staticmethod
    def load(
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ) -> BaseAction:

        # Don't allow relative references
        if ".." in definition.kind:
            raise NotImplementedError("Unknown action '{}'".format(definition.kind))

        module_path, class_name = ActionFactory.get_module_and_class_name(
            definition.kind
        )

        # Import the module and instantiate the action class
        try:
            action_module = importlib.import_module(module_path)

            # Get the action class
            klass = getattr(action_module, class_name)

            # Instantiate the action with detailed error handling
            try:
                action = klass(definition, context, deployment_details)
                log.debug("Successfully created action: {}", definition.name)
                return action

            except ValidationError as e:
                # Pydantic validation error - preserve all details
                error_details = {
                    "action_name": definition.name,
                    "action_kind": definition.kind,
                    "validation_errors": e.errors(),
                    "input_data": definition.params,
                    "error_count": e.error_count(),
                }
                log.error(
                    "Pydantic validation failed for action '{}': {}", definition.name, e
                )
                log.debug("Detailed validation errors: ", details=error_details)

                # Create a comprehensive error message
                error_summary = (
                    f"Action '{definition.name}' parameter validation failed:\n"
                )
                for error in e.errors():
                    field_path = " -> ".join(str(loc) for loc in error["loc"])
                    error_summary += f"  Field '{field_path}': {error['msg']}\n"

                raise RuntimeError(error_summary) from e

            except Exception as e:
                # Other initialization errors
                error_details = {
                    "action_name": definition.name,
                    "action_kind": definition.kind,
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "input_data": definition.params,
                }
                log.error("Error initializing action '{}': {}", definition.name, e)
                log.debug(
                    "Action initialization error details: ", details=error_details
                )

                # Preserve the original exception info
                raise RuntimeError(
                    f"Failed to initialize action '{definition.name}': {str(e)}"
                ) from e

        except AttributeError as e:
            # Class not found error
            log.error("Action class '{}' not found in module: {}", class_name, e)
            raise RuntimeError(f"Action class '{class_name}' not found") from e
        except Exception as e:
            # Module loading or other unexpected errors
            log.error("Unexpected error creating action '{}': {}", definition.name, e)
            raise RuntimeError(
                f"Unexpected error creating action '{definition.name}': {str(e)}"
            ) from e

    @staticmethod
    def get_action_class(action_type: str):
        """
        Get the action class for a given action type.
        :param action_type: The action type to get the class for.
        :return: The action class if found, None otherwise.
        """
        for prefix, action_class in valid_actions:
            if action_type == prefix:
                return action_class
        return None

    @staticmethod
    def get_action_spec(**action_spec):

        kind = action_spec.get("kind") or action_spec.get("Kind")
        if not kind:
            raise ValueError("Action spec must contain a 'kind' field")

        action_class = ActionFactory.get_action_class(kind)
        if action_class is None:
            raise ValueError(f"Action class for kind '{kind}' not found")

        if hasattr(action_class, "generate_spec"):
            return action_class.generate_spec(action_spec)

        raise ValueError(
            f"Action class '{action_class.__name__}' does not have a 'generate_spec' method"
        )

    @staticmethod
    def is_valid_action(action_type: str) -> bool:
        """
        Check if the action name is valid.
        :param action_label: The action name to check.
        :return: True if the action name is valid, False otherwise.
        """
        return any(action_type == prefix for prefix, _ in valid_actions)
