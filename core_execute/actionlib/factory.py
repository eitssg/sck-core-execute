"""Factory module for creating action instances from action definitions."""

from typing import Any

import importlib
import re
import os

import core_logging as log

from core_framework.models import ActionSpec
from pydantic import ValidationError
from core_execute.actionlib.action import BaseAction

from core_framework.models import DeploymentDetails


class ActionFactory:
    """Factory for dynamically creating action instances from action specifications.

    The ActionFactory provides a centralized mechanism for instantiating action classes
    based on their kind specification. It handles dynamic module loading, class resolution,
    parameter validation, and comprehensive error reporting for action creation failures.

    **Key Features:**

    - Dynamic action class loading based on kind specification
    - Automatic module path resolution from action kinds
    - Parameter validation with detailed error reporting
    - Comprehensive exception handling and logging
    - Support for AWS and custom action namespaces

    **Action Kind Format:**

    Action kinds follow a hierarchical namespace pattern:

    - **AWS actions**: ``AWS::ActionName`` (e.g., ``AWS::CreateStack``)
    - **Custom actions**: ``namespace::ActionName`` (e.g., ``Docker::BuildImage``)
    - **Simple actions**: ``ActionName`` (e.g., ``DeployApplication``)

    **Module Resolution:**

    The factory automatically resolves action kinds to module paths:

    - ``AWS::CreateStack`` → ``core_execute.actionlib.actions.aws.create_stack``
    - ``Docker::BuildImage`` → ``core_execute.actionlib.actions.docker.build_image``
    - ``DeployApp`` → ``core_execute.actionlib.actions.deploy_app``

    **Class Name Convention:**

    Action classes must follow the naming convention:
    ``{ActionName}Action`` (e.g., ``CreateStackAction``, ``DeployAppAction``)

    **Error Handling:**

    The factory provides detailed error reporting for:

    - Module loading failures
    - Class resolution failures
    - Parameter validation errors
    - Action initialization failures

    **Examples:**

    Basic action creation::

        factory = ActionFactory()
        action_spec = ActionSpec(
            name="create-vpc",
            kind="AWS::CreateStack",
            params={"StackName": "my-vpc", "Region": "us-east-1"}
        )
        action = factory.load(action_spec, context, deployment_details)

    Checking action validity::

        is_valid = ActionFactory.is_valid_action("AWS::CreateStack")
        if is_valid:
            action_class = ActionFactory.get_action_class("AWS::CreateStack")

    **Security Considerations:**

    - Prevents relative path traversal attacks (``..`` not allowed in kinds)
    - Only loads modules from the predefined action namespace
    - Validates action classes inherit from BaseAction

    **Thread Safety:**

    The ActionFactory is stateless and thread-safe. All methods are static
    and can be called concurrently without synchronization concerns.

    Attributes
    ----------
    ACTION_CLASS_NAME_SUFFIX : str
        The suffix appended to action names to form class names ("Action")

    See Also
    --------
    BaseAction : Base class for all actions
    ActionSpec : Action specification model
    DeploymentDetails : Deployment context information
    """

    ACTION_CLASS_NAME_SUFFIX: str = "Action"

    @staticmethod
    def __camel_to_snake_case(string: str) -> str:
        """Convert CamelCase string to snake_case format.

        Transforms action kind components from CamelCase to snake_case
        for use in module path resolution. Handles mixed case scenarios
        and preserves numeric characters.

        Parameters
        ----------
        string : str
            The CamelCase string to convert

        Returns
        -------
        str
            The converted snake_case string

        Examples
        --------
        Convert action names to module paths::

            >>> ActionFactory._ActionFactory__camel_to_snake_case("CreateStack")
            'create_stack'
            >>> ActionFactory._ActionFactory__camel_to_snake_case("S3BucketPolicy")
            's3_bucket_policy'
            >>> ActionFactory._ActionFactory__camel_to_snake_case("DeployV2App")
            'deploy_v2_app'

        Notes
        -----
        This is a private method used internally for module path resolution.
        The double underscore prefix makes it name-mangled in Python.
        """
        # Separate capitals after a non-capital with an underscore (ignores special characters)
        return re.sub("([a-z0-9])([A-Z])", r"\1_\2", string).lower()

    @staticmethod
    def __snake_to_camel_case(name: str) -> str:
        """
        Convert the strring to PascalCase

        Parameters
        ----------
        name : str
            The snake_case to convert

        Returns
        -------
        str
            The PascalCase representation of the input string

        """
        return name.title().replace("_", "")

    @staticmethod
    def __get_module_class(action_type: str) -> tuple[str, str]:
        """Resolve action kind to module path and class name.

        Parses the action kind specification and generates the corresponding
        module path and class name for dynamic loading. Handles namespace
        prefixes and converts naming conventions appropriately.

        Parameters
        ----------
        action_type : str
            The action kind specification (e.g., "AWS::CreateStack")

        Returns
        -------
        tuple[str, str]
            A tuple containing (module_path, class_name)

            - module_path: Full Python module path for import
            - class_name: Class name to instantiate from the module

        Examples
        --------
        Resolve AWS action kinds::

            >>> ActionFactory._ActionFactory__get_module_class("AWS::CreateStack")
            ('core_execute.actionlib.actions.aws.create_stack', 'CreateStackAction')

        Resolve custom action kinds::

            >>> ActionFactory._ActionFactory__get_module_class("Docker::BuildImage")
            ('core_execute.actionlib.actions.docker.build_image', 'BuildImageAction')

        Notes
        -----
        This is a private method used internally for action resolution.
        The generated paths follow the project's action organization structure.
        """
        actions_path: list[str] = ["core_execute", "actionlib", "actions"]
        actions_root = os.path.join(os.path.dirname(__file__), "actions")
        action_type = action_type.replace(
            "-", "_"
        )  # Normalize dashes to underscores.  create-stack -> create_stack

        # if the action_type is already lowercase snake_case, then search the actions_path and all subdirectories for the filename to build the module_path
        if re.match(r"^[a-z]+(?:_[a-z]+)*$", action_type):
            module_path = None
            for root, dirs, files in os.walk(actions_root):
                for filename in files:
                    if filename == f"{action_type}.py":
                        rel_path = os.path.relpath(
                            os.path.join(root, filename), actions_root
                        )
                        # Remove .py extension and convert path separators to dots
                        module_path = (
                            ".".join(actions_path)
                            + "."
                            + rel_path[:-3].replace(os.sep, ".")
                        )
                        break
                if module_path:
                    break
            # create the class name by converting the action_type to PascalCase and appending the ACTION_CLASS_NAME_SUFFIX
            class_name = (
                ActionFactory.__snake_to_camel_case(action_type)
                + ActionFactory.ACTION_CLASS_NAME_SUFFIX
            )
            return module_path, class_name
        elif "::" in action_type:
            # Work out the class name and module path from the action kind
            split_type = action_type.split("::")
            class_name = split_type[-1] + ActionFactory.ACTION_CLASS_NAME_SUFFIX
            module_path = ActionFactory.__camel_to_snake_case(
                ".".join(actions_path + split_type)
            )
            return module_path, class_name

    @staticmethod
    def get_action_class(action_type: str) -> type[BaseAction]:
        """Dynamically load and return the action class for the specified kind.

        Resolves the action kind to a module path, imports the module,
        and returns the corresponding action class. This method handles
        the dynamic loading aspect of the factory pattern.

        Parameters
        ----------
        action_type : str
            The action kind specification to resolve

        Returns
        -------
        type[BaseAction]
            The action class type that can be instantiated

        Raises
        ------
        ModuleNotFoundError
            If the resolved module path does not exist
        AttributeError
            If the action class is not found in the module
        ImportError
            If there are import-time errors in the module

        Examples
        --------
        Load action classes dynamically::

            >>> action_class = ActionFactory.get_action_class("AWS::CreateStack")
            >>> isinstance(action_class(), BaseAction)
            True

        Get class for validation::

            >>> klass = ActionFactory.get_action_class("AWS::DeployStack")
            >>> action = klass(spec, context, deployment_details)

        Notes
        -----
        This method performs actual module imports and should be used
        carefully in performance-critical code paths. Consider caching
        results if the same action types are used repeatedly.
        """

        module_path, class_name = ActionFactory.__get_module_class(action_type)
        action_module = importlib.import_module(module_path)
        klass = getattr(action_module, class_name)
        if klass is None:
            raise RuntimeError(
                f"Action class '{class_name}' not found in module '{module_path}'"
            )
        if not issubclass(klass, BaseAction):
            raise TypeError(
                f"Action class '{class_name}' does not inherit from BaseAction"
            )
        return klass

    @staticmethod
    def is_valid_action(action_type: str) -> bool:
        """Validate whether the specified action kind represents a valid action.

        Attempts to load the action class and verifies it inherits from
        BaseAction. This method provides a safe way to check action
        validity without raising exceptions.

        Parameters
        ----------
        action_type : str
            The action kind specification to validate

        Returns
        -------
        bool
            True if the action kind is valid and loadable, False otherwise

        Examples
        --------
        Validate action kinds before use::

            >>> ActionFactory.is_valid_action("AWS::CreateStack")
            True
            >>> ActionFactory.is_valid_action("InvalidAction")
            False

        Pre-flight validation in action loading::

            if ActionFactory.is_valid_action(spec.kind):
                action = ActionFactory.load(spec, context, deployment_details)
            else:
                raise ValueError(f"Invalid action kind: {spec.kind}")

        Notes
        -----
        This method catches and suppresses all exceptions during validation.
        It should be used for validation only, not for actual action loading
        where detailed error information is needed.
        """
        try:
            klass = ActionFactory.get_action_class(action_type)
            return klass is not None and issubclass(klass, BaseAction)
        except Exception:
            return False

    @staticmethod
    def load(
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ) -> BaseAction:
        """Create and return a fully initialized action instance.

        This is the main factory method that handles the complete action
        creation process including class loading, parameter validation,
        initialization, and comprehensive error handling.

        The method provides detailed error reporting for all failure modes
        and ensures that action creation issues are properly diagnosed.

        Parameters
        ----------
        definition : ActionSpec
            The action specification containing kind, name, and parameters
        context : dict[str, Any]
            Template rendering context with deployment variables and outputs
        deployment_details : DeploymentDetails
            Deployment context and metadata for the action

        Returns
        -------
        BaseAction
            A fully initialized action instance ready for execution

        Raises
        ------
        NotImplementedError
            If the action kind contains relative path references (..)
        RuntimeError
            If the action class is not found in the resolved module
        TypeError
            If the action class does not inherit from BaseAction
        ModuleNotFoundError
            If the resolved module path does not exist
        AttributeError
            If the action class is not found in the module
        ImportError
            If there are import-time errors in the module
        RuntimeError
            For action initialization failures, including:

            - Parameter validation failures (with detailed field errors)
            - Action constructor exceptions
            - Any other initialization errors

        Examples
        --------
        Create a simple action::

            action_spec = ActionSpec(
                name="namespace:action/create-vpc",
                kind="AWS::CreateStack",
                params={
                    "StackName": "my-vpc-stack",
                    "Region": "us-east-1",
                    "TemplateBody": "..."
                }
            )

            action = ActionFactory.load(action_spec, context, deployment_details)

        Handle creation errors::

            try:
                action = ActionFactory.load(spec, context, deployment_details)
            except NotImplementedError as e:
                log.error(f"Invalid action kind: {e}")
            except (ModuleNotFoundError, AttributeError, ImportError) as e:
                log.error(f"Action class loading failed: {e}")
            except (RuntimeError, TypeError) as e:
                log.error(f"Action creation failed: {e}")

        **Error Message Examples:**

        Parameter validation failure::

            Action 'create-vpc' parameter validation failed:
            Field 'StackName': field required
            Field 'Region': ensure this value has at least 1 characters

        Action initialization failure::

            Failed to initialize action 'create-vpc': Invalid template syntax

        Class not found::

            Action class 'InvalidActionAction' not found

        Module not found::

            No module named 'core_execute.actionlib.actions.aws.invalid_action'

        **Security Considerations:**

        - Prevents directory traversal attacks by rejecting kinds with ".."
        - Only loads modules from the predefined action namespace
        - Validates all parameters before action initialization

        **Performance Considerations:**

        - Module imports are cached by Python's import system
        - Consider pre-loading frequently used action classes
        - Parameter validation occurs during initialization

        Notes
        -----
        This method performs comprehensive error handling and logging.
        Exceptions from get_action_class() are allowed to propagate, while
        action initialization exceptions are wrapped in RuntimeError with
        detailed context information to aid in debugging.
        """

        # Don't allow relative references
        if ".." in definition.kind:
            raise NotImplementedError("Unknown action '{}'".format(definition.kind))

        # Get the action class.
        # raises RuntimeError if not found
        # raises TypeError if class does not inherit from BaseAction
        klass = ActionFactory.get_action_class(definition.kind)

        try:
            # Instantiate the action with detailed error handling.
            # Definition param attributes are validated to the action
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
                "Parameter validation failed for action '{}': {}", definition.name, e
            )
            log.debug("Detailed validation errors: ", details=error_details)

            # Create a comprehensive error message
            error_summary = f"Action '{definition.name}' parameter validation failed:\n"
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
            log.debug("Action initialization error details: ", details=error_details)

            # Preserve the original exception info
            raise RuntimeError(
                f"Failed to initialize action '{definition.name}': {str(e)}"
            ) from e
