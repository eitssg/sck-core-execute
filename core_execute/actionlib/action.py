"""Defines the BaseActions abstraction for all actions."""

from typing import Any, Generic, Self, Optional, TypeVar
import traceback
import sys
import os
import enum

import core_logging as log

from core_framework.models import (
    ActionResource,
    ActionMetadata,
    ActionSpec,
    DeploymentDetails,
)

from core_renderer import Jinja2Renderer

from core_framework.constants import SCOPE_BUILD, SCOPE_COMPONENT
from core_framework.status import RELEASE_IN_PROGRESS
from core_framework.models import HookResource

from core_db.dbhelper import update_status, update_item

from .hooks import ActionHook, HookFactory

STATUS_CODE = "StatusCode"
STATUS_REASON = "StatusReason"

LC_HOOK_PENDING = "Pending"
LC_HOOK_FAILED = "Failed"
LC_HOOK_RUNNING = "Running"
LC_HOOK_COMPLETE = "Complete"

NO_DEFAULT_PROVIDED = "_!NO!DEFAULT!PROVIDED!_"


class StatusCode(str, enum.Enum):
    """Action execution status codes.

    PENDING: Action has not started execution
    RUNNING: Action is currently executing
    COMPLETE: Action finished successfully
    FAILED: Action encountered an error
    """

    PENDING = "pending"
    RUNNING = "running"
    COMPLETE = "complete"
    FAILED = "failed"
    SKIPPED = "skipped"
    BLOCKED = "blocked"


SpecType = TypeVar("SpecType", bound=ActionSpec)


class BaseAction(Generic[SpecType]):
    """Base class for all Simple Cloud Kit actions.

    Provides the foundation for implementing custom deployment actions with:
    - Action lifecycle management (pending → running → complete/failed)
    - Jinja2 template rendering for dynamic configuration
    - Output and state variable management with namespace isolation
    - Conditional execution based on Jinja2 expressions
    - Lifecycle hooks for status updates and notifications
    - Comprehensive error handling and logging

    All action subclasses must inherit from BaseAction and implement:
    - _resolve(): Prepare action for execution (resolve dependencies, validate config)
    - _execute(): Main action logic (deploy resources, update configuration)
    - _check(): Validate prerequisites before execution
    - _cancel(): Cancel a running action gracefully
    - _unexecute(): Rollback/undo action changes

    Args:
        definition (ActionResource): ActionResource specification from deployspec.yaml
        context (dict): Jinja2 rendering context with all deployment variables
        deployment_details (DeploymentDetails): Client/portfolio/app/branch/build information

    Example:
        class MyAction(BaseAction):
            def _resolve(self):
                self.set_state("config_validated", True)

            def _execute(self):
                self.set_running("Deploying resources")
                result = self.deploy_something()
                self.set_output("endpoint_url", result.url)
                self.set_complete("Deployment successful")
    """

    name: str
    """Full action identifier (e.g., 'namespace/action-name')"""

    context: dict[str, Any]
    """Jinja2 rendering context containing all deployment variables and outputs"""

    action_name: str
    """Short action name without namespace (e.g., 'action-name')"""

    output_namespace: str | None
    """Namespace for saved outputs that other actions can reference (e.g., 'namespace:output')"""

    state_namespace: str
    """Namespace for internal state variables (e.g., 'namespace:var')"""

    kind: str
    """Action type/kind (e.g., 'create-stack', 'invoke-lambda')"""

    condition: str
    """Jinja2 condition expression that determines if action should execute"""

    after: list[str]
    """List of action names that must complete before this action can run"""

    lifecycle_hooks: list[HookResource]
    """Status notification hooks for action state changes"""

    deployment_details: DeploymentDetails
    """Deployment metadata (client, portfolio, app, branch, build)"""

    renderer: Jinja2Renderer
    """Template renderer using the action's context for variable substitution"""

    spec: SpecType

    def _execute(self):
        """Execute the main action logic.

        Must be implemented in subclasses. Called after _resolve() when the action
        condition evaluates to true. Should use self.set_running(), self.set_complete(),
        or self.set_failed() to manage execution state.

        Raises:
            NotImplementedError: Must be implemented in action subclasses
        """
        raise NotImplementedError("Must implement in subclass")

    def _check(self):
        """Validate action prerequisites and readiness.

        Must be implemented in subclasses. Called during check() to validate
        that the action can be executed successfully. Should verify dependencies,
        permissions, and configuration.

        Raises:
            NotImplementedError: Must be implemented in action subclasses
        """
        raise NotImplementedError("Must implement in subclass")

    def _resolve(self):
        """Resolve dependencies and prepare for execution.

        Must be implemented in subclasses. Called before _execute() and _check()
        to resolve any dependencies, render templates, and prepare the action
        for execution.

        Raises:
            NotImplementedError: Must be implemented in action subclasses
        """
        raise NotImplementedError("Must implement in subclass")

    def _cancel(self):
        """Cancel a running action gracefully.

        Must be implemented in subclasses. Called to cancel an action that is
        currently running. Should clean up resources and set the action to
        failed state with appropriate reason.

        Raises:
            NotImplementedError: Must be implemented in action subclasses
        """
        raise NotImplementedError("Must implement in subclass")

    def _unexecute(self):
        """Rollback or undo action changes.

        Must be implemented in subclasses. Called to reverse the effects of
        a previously executed action. Should restore the system to its
        pre-execution state.

        Raises:
            NotImplementedError: Must be implemented in action subclasses
        """
        raise NotImplementedError("Must implement in subclass")

    def __init__(
        self,
        definition: ActionResource,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ) -> None:
        """Initialize a new BaseAction instance.

        Sets up the action with configuration from the ActionResource, initializes
        the Jinja2 renderer, and resolves action identity using either modern
        metadata or legacy name fields.

        Args:
            definition: Action specification from deployspec.yaml
            context: Jinja2 rendering context with deployment variables
            deployment_details: Client/portfolio/app/branch/build information
        """
        log.trace("BaseAction.__init__()")

        # All actions can use the Jinja2 renderer to parse CloudFormation.j2 templates
        self.renderer = Jinja2Renderer()

        # Extract action details from the definition
        self.definition = definition
        self.context = context
        self.deployment_details = deployment_details
        self.condition = definition.condition or "true"
        self.before = definition.before or []
        self.after = definition.after or []
        self.lifecycle_hooks = definition.lifecycle_hooks or []

        metadata = self.definition.metadata
        if metadata:
            save_outputs = metadata.save_outputs if metadata.save_outputs is not None else True

        self.save_outputs = save_outputs
        self.name = self.definition.action_key
        self.action_name = self.definition.action_name
        self.output_namespace = self.definition.output_namespace
        self.state_namespace = self.definition.state_namespace

        log.debug("Action name is: {}", self.name)
        log.debug("Action output namespace is: {}", self.output_namespace)
        log.debug("Action state namespace is: {}", self.state_namespace)
        log.debug("Action context is: ", details=self.context)

    def _create_metadata_from_legacy_name(self, namespace: str | None, action_name: str):
        """Create metadata structure from legacy name for forward compatibility.

        Creates an ActionMetadata instance from parsed legacy name components
        to enable modern metadata-based features for legacy actions.

        Args:
            namespace: Parsed namespace from legacy name (can be None)
            action_name: Parsed action name from legacy name
        """
        # Create metadata if it doesn't exist
        if not self.definition.metadata:
            self.definition.metadata = ActionMetadata(
                name=action_name,
                namespace=namespace,
                description=f"Auto-generated metadata for legacy action: {self.definition.name}",
            )
            log.debug(
                "Created metadata from legacy name: namespace='{}', name='{}'",
                namespace,
                action_name,
            )
        else:
            # Update existing metadata if fields are missing
            if not self.definition.metadata.name:
                self.definition.metadata.name = action_name
            if not self.definition.metadata.namespace:
                self.definition.metadata.namespace = namespace

    def is_pending(self) -> bool:
        """Check if the action is in the initial pending state.

        Returns:
            True if the action has not started execution
        """
        return self.__get_status_code() == StatusCode.PENDING

    def is_failed(self) -> bool:
        """Check if the action is in the failed state.

        Returns:
            True if the action encountered an error during execution
        """
        return self.__get_status_code() == StatusCode.FAILED

    def is_running(self) -> bool:
        """Check if the action is currently executing.

        Returns:
            True if the action is currently running
        """
        return self.__get_status_code() == StatusCode.RUNNING

    def is_complete(self) -> bool:
        """Check if the action completed successfully.

        Returns:
            True if the action finished execution without errors
        """
        return self.__get_status_code() == StatusCode.COMPLETE

    def set_pending(self, reason: str | None = None):
        """Set the action status to pending with the specified reason.

        Updates the action state to pending, executes lifecycle hooks, and logs
        the status change. Ignores duplicate state updates with the same reason.

        Args:
            reason: Description of why the action is pending
        """
        log.trace("Setting action to pending - {}", reason)

        if reason is None:
            reason = "Action is pending."

        # Ignore duplicate state updates
        if self.is_pending() and self.__get_status_reason() == reason:
            log.trace("Action is already pending - {}", reason)
            return

        self.set_status(StatusCode.PENDING, reason)

        log.debug("Action is pending - {}", reason)

    def set_failed(self, reason: str):
        """Set the action status to failed with the specified reason.

        Updates the action state to failed, executes lifecycle hooks, and logs
        the failure. Ignores duplicate state updates with the same reason.

        Args:
            reason: Detailed reason why the action failed
        """
        log.trace("Setting action to failed - {}", reason)

        # Ignore duplicate state updates
        if self.is_failed() and self.__get_status_reason() == reason:
            log.trace("Action is already failed - {}", reason)
            return

        # Log the state change
        log.debug("Action has failed - {}", reason)

        # Execute lifecycle hooks
        if self._execute_lifecycle_hooks(LC_HOOK_FAILED, reason):
            # Update the context with the new state
            self.set_status(StatusCode.FAILED, reason)
        else:
            log.error("Failed to execute lifecycle hooks for action failure - {}", reason)

        log.trace("Action set to failed - {}", reason)

    def set_running(self, reason: str):
        """Set the action status to running with the specified reason.

        Updates the action state to running, executes lifecycle hooks, and logs
        the status change. Ignores duplicate state updates with the same reason.

        Args:
            reason: Description of what the action is currently doing
        """
        log.trace("Setting action to running - {}", reason)

        # Ignore duplicate state updates
        if self.is_running() and self.__get_status_reason() == reason:
            log.trace("Action is already running - {}", reason)
            return

        # Log the state change
        log.debug(reason or "Action is running")

        # Execute lifecycle hooks
        self._execute_lifecycle_hooks(LC_HOOK_RUNNING, reason)

        # Update the context with the new state
        self.set_status(StatusCode.RUNNING, reason)

        log.trace("Action set to running - {}", reason)

    def set_complete(self, reason: str | None = None):
        """Set the action status to complete with optional reason.

        Updates the action state to complete, executes lifecycle hooks, and logs
        the completion. Ignores duplicate state updates with the same reason.

        Args:
            reason: Optional description of completion (defaults to "Action finished.")
        """
        if reason is None:
            reason = "Action finished."

        log.trace("Setting action to complete - {}", reason)

        # Ignore duplicate state updates
        if self.is_complete() and self.__get_status_reason() == reason:
            log.trace("Action is already complete - {}", reason)
            return

        # Log the state change
        log.debug("Action is complete - {}", reason)

        # Execute lifecycle hooks
        self._execute_lifecycle_hooks(LC_HOOK_COMPLETE, reason)

        # Update the context with the new state
        self.set_status(StatusCode.COMPLETE, reason)

        log.trace("Action set to complete - {}", reason)

    def set_status(self, status: StatusCode, reason: str | None = None) -> None:
        """Set the action status to the specified code and reason.

        Updates the action state to the given status code, executes lifecycle
        hooks if applicable, and logs the status change. Ignores duplicate
        state updates with the same reason.

        Args:
            status: New status code (PENDING, RUNNING, COMPLETE, FAILED)
            reason: Description of the status change
        """
        log.trace("Setting action status to {} - {}", status, reason)

        if reason is None:
            reason = f"Action status set to {status}."

        # Ignore duplicate state updates
        if self.__get_status_code() == status and self.__get_status_reason() == reason:
            log.trace("Action is already {} - {}", status, reason)
            return

        # Log the state change
        log.debug("Action status set to {} - {}", status, reason)

        # Update the context with the new state
        self.__set_context(self.name, STATUS_CODE, status.value)
        self.__set_context(self.name, STATUS_REASON, reason)

    def set_skipped(self, reason: str):
        """Set the action status to skipped with the specified reason.

        Marks the action as complete but skipped, typically when conditions
        are not met. Does not execute lifecycle hooks for skipped actions.

        Args:
            reason: Explanation of why the action was skipped
        """
        log.trace("Setting action to skipped - {}", reason)

        # Ignore duplicate state updates
        if self.is_complete() and self.__get_status_reason() == reason:
            log.trace("Action is already complete - {}", reason)
            return

        # Log the state change
        log.debug("Action has been skipped - {}", reason)

        self.set_status(StatusCode.COMPLETE, reason)

        log.trace("Action set to skipped - {}", reason)

    def set_output(self, name: str, value: Any):
        """Set an output variable that other actions can reference.

        Saves the output to both the output namespace (if SaveOutputs is enabled)
        and the state namespace. Other actions can reference outputs using
        Jinja2 expressions like {{ namespace.variable_name }}.

        Args:
            name: Variable name within the action's output namespace
            value: Value to store (can be any serializable type)
        """
        log.trace("Setting output '{}' = '{}'", name, value)

        # Set output variable (if user chose to save outputs)
        if self.output_namespace:
            log.debug("Setting output '{}/{}' = '{}'", self.output_namespace, name, value)
            self.__set_context(self.output_namespace, name, value)

        # Set state variable
        self.__set_context(self.state_namespace, name, value)

        log.trace("Output '{}' set to '{}'", name, value)

    def get_output(self, name: str, default: Any = NO_DEFAULT_PROVIDED) -> str | None:
        """Get an output variable from the action's output namespace.

        Retrieves a previously set output variable. Returns None if no output
        namespace exists (SaveOutputs disabled).

        Args:
            name: Name of the output variable to retrieve
            default: Default value if variable not found

        Returns:
            Value of the output variable, or None if no output namespace

        Raises:
            KeyError: If variable not found and no default provided
        """
        log.trace("Getting output '{}'", name)

        if self.output_namespace:
            return self.__get_context(self.output_namespace, name, default)
        return None

    def set_state(self, name: str, value: Any):
        """Set an internal state variable for this action.

        State variables are used for internal action bookkeeping and are
        stored in the action's state namespace. Unlike outputs, state
        variables are not typically referenced by other actions.

        Args:
            name: Name of the state variable
            value: Value to store (can be any serializable type)
        """
        log.trace("Setting state '{}' = '{}'", name, value)
        self.__set_context(self.state_namespace, name, value)

    def get_status(self) -> StatusCode:
        """Get the current action status code.

        Returns:
            Current status code (PENDING, RUNNING, COMPLETE, FAILED)
        """
        return self.__get_status_code()

    def get_state(self, name: str, default: Any = None) -> str:
        """Get an internal state variable for this action.

        Retrieves a previously set state variable from the action's
        state namespace.

        Args:
            name: Name of the state variable to retrieve
            default: Default value if variable not found

        Returns:
            Value of the state variable

        Raises:
            KeyError: If variable not found and no default provided
        """
        log.trace("Getting state '{}'", name)
        return self.__get_context(self.state_namespace, name, default)

    def execute(self) -> Self:
        """Execute the action after evaluating its condition.

        Evaluates the action's condition using Jinja2 templating. If true,
        calls _resolve() then _execute(). If false, marks the action as skipped.
        Handles all exceptions and sets failed status with detailed error info.

        Returns:
            Self for method chaining
        """
        try:
            # Temporarily set the logger identity to this action's name
            log.set_identity(self.name)

            log.trace("Executing action for {}", self.name)

            # Render the action condition, and see if it evaluates to true
            condition_result = self.renderer.render_string("{{ " + self.condition + " }}", self.context)

            if condition_result.lower() == "true":
                # Condition is true, execute the action
                self._resolve()
                self._execute()
            else:
                # Condition is false, skip the action
                self.set_skipped("Condition evaluated to '{}'".format(condition_result))

            log.trace("Action executed for {}", self.name)

        except Exception as e:
            # Something went wrong (internal error)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            if exc_type is None:
                exc_type = type(e)
            if exc_tb and exc_tb.tb_frame:
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                lineno = exc_tb.tb_lineno
            else:
                fname = "Unknown"
                lineno = -1
            tb_str = "".join(traceback.format_exception(exc_type, exc_obj, exc_tb))
            self.set_failed(
                "Internal error {} in {} at {} - {}\nTraceback:\n{}".format(exc_type.__name__, fname, lineno, str(e), tb_str)
            )
            log.error(
                "Internal error {} in {} at {} - {}",
                exc_type.__name__,
                fname,
                lineno,
                str(e),
            )

        finally:
            # Reset the logger identity to base value
            log.trace("Resetting identity")
            log.reset_identity()

        return self

    def check(self) -> Self:
        """Check if the action is ready to run.

        Calls _resolve() then _check() to validate prerequisites and readiness.
        Handles all exceptions and sets failed status with detailed error info.

        Returns:
            Self for method chaining
        """
        try:
            # Temporarily set the logger identity to this action's name
            log.set_identity(self.name)

            log.debug("Checking action for {}", self.name)

            self._resolve()
            self._check()

            log.trace("Action checked for {}", self.name)

        except Exception as e:
            # Something went wrong (internal error)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            if exc_type is None:
                exc_type = type(e)
            if exc_tb and exc_tb.tb_frame:
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                lineno = exc_tb.tb_lineno
            else:
                fname = "Unknown"
                lineno = -1
            tb_str = "".join(traceback.format_exception(exc_type, exc_obj, exc_tb))
            self.set_failed(
                "Internal error {} in {} at {} - {}\nTraceback:\n{}".format(exc_type.__name__, fname, lineno, str(e), tb_str)
            )
            log.error(
                "Internal error {} in {} at {} - {}",
                exc_type.__name__,
                fname,
                lineno,
                str(e),
            )

        finally:
            # Reset the logger identity to base value
            log.reset_identity()

        return self

    def __get_status_code(self) -> StatusCode:
        """Get the current status code from context."""
        return StatusCode(self.__get_context(self.name, STATUS_CODE, StatusCode.PENDING))

    def __get_status_reason(self):
        """Get the current status reason from context."""
        return self.__get_context(self.name, STATUS_REASON, None)

    def __get_context(self, prn: str, name: str, default: Any = NO_DEFAULT_PROVIDED) -> Any:
        """Get a value from the action context.

        Args:
            prn: Namespace/prefix for the context key
            name: Variable name within the namespace
            default: Default value if key not found

        Returns:
            Context value (can be list, str, int, float, dict, datetime, or None)

        Raises:
            KeyError: If key not found and no default provided
        """
        log.trace("Getting context '{}' for '{}'", name, prn)
        key = "{}/{}".format(prn, name)

        if self.context and key in self.context:
            return self.context[key]
        else:
            if default == NO_DEFAULT_PROVIDED:
                raise KeyError("Key '{}' is not in the context and no default was provided".format(name))
            else:
                return default

    def __set_context(self, prn: str, name: str, value: Any):
        """Set a value in the action context.

        Args:
            prn: Namespace/prefix for the context key
            name: Variable name within the namespace
            value: Value to store in context
        """
        key = "{}/{}".format(prn, name)
        self.context[key] = value
        """Update status in the database for the specified identity.

        Args:
            identity: PRN identity to update
            status: New status value
            message: Status message
            details: Additional status details
        """
        try:

            identity = self.deployment_details.get_identity()
            status = self.__get_status_code().value
            message = self.__get_status_reason() or ""

            details = self.definition.model_dump()

            # Log the status
            log.set_identity(identity)

            scope = self.deployment_details.get_scope()
            # Build PRN
            if scope == SCOPE_BUILD:

                # Update the build status
                build_prn = self.deployment_details.get_build_prn()
                update_status(prn=build_prn, status=status, message=message, details=details)

                # If a new build is being released, update the branch's released_build_prn pointer
                if status == RELEASE_IN_PROGRESS:
                    branch_prn = self.deployment_details.get_branch_prn()
                    update_item(prn=branch_prn, released_build_prn=build_prn)

            # Component PRN
            if scope == SCOPE_COMPONENT:

                # Update the component status
                component_prn = self.deployment_details.get_component_prn()
                update_status(prn=component_prn, status=status, message=message, details=details)

                # If component has failed, update the build status to failed
                if "_FAILED" in status:
                    build_prn = self.deployment_details.get_build_prn()
                    update_status(prn=build_prn, status=status)

        except Exception as e:
            log.warn("Failed to update status via API - {}", e)

        finally:
            log.reset_identity()

    def __repr__(self) -> str:
        """String representation for debugging."""
        return "{}({})".format(type(self).__name__, self.name)

    def __str__(self) -> str:
        """String representation for display."""
        return "{}({})".format(type(self).__name__, self.name)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> ActionSpec:
        """Generate validated action parameters for this action type.

        Subclasses should override this to return a validated parameter set
        specific to their action type.

        Args:
            **kwargs: Parameter values to validate

        Returns:
            Validated ActionSpec instance
        """
        return ActionSpec(**kwargs)

    @classmethod
    def generate_action_resource(cls, **kwargs) -> ActionResource:
        """Generate an ActionResource for this action type.

        Subclasses should override this to return an ActionResource with
        appropriate defaults and validation for their specific action type.

        Args:
            **kwargs: ActionResource values to override

        Returns:
            ActionResource instance for this action type
        """
        return ActionResource(**kwargs)

    def can_initialize(self) -> bool:
        """
        Check if action can be reinitialized for rerun.

        Default implementation allows reinitialization unless action
        is in a critical state that cannot be reset.

        Returns:
            bool: True if action can be reinitialized, False otherwise
        """
        return True

    def initialize(self) -> bool:
        """
        Initialize action for rerun.

        This method should reset action state to allow clean rerun.
        Default implementation clears state and output data.

        Returns:
            bool: True if initialization was successful, False otherwise.

        """
        log.debug("Initializing action {} for run", self.name)

        # Clear action-specific state (keep deployment context)
        state_keys_to_clear = []
        for key in self.context.keys():
            if key.startswith(f"{self.name}/"):
                state_keys_to_clear.append(key)

        for key in state_keys_to_clear:
            self.context.pop(key, None)
            log.trace("Cleared state key: {}", key)

        return True

    def can_execute(self) -> bool:
        """
        Check if action can execute based on current conditions.

        This is called before execution to validate prerequisites.
        Default implementation always returns True.

        Returns:
            bool: True if action can execute, False otherwise
        """
        return True

    def _execute_lifecycle_hooks(self, hook_type: str, reason: str) -> bool:
        """Execute lifecycle hooks of the specified type.

        Args:
            hook_type: Lifecycle hook type (e.g., "running", "complete", "failed")
            reason: Reason for the lifecycle event
        """
        log.trace(
            "Executing lifecycle hooks of type '{}' for action '{}'",
            hook_type,
            self.name,
        )

        all_success = True
        for hook_resource in self.lifecycle_hooks:
            all_success &= self._execute_lifecycle_hook(hook_type, hook_resource, reason)

        return all_success

    def _execute_lifecycle_hook(self, hook_type: str, hook_resource: HookResource, reason: str) -> bool:

        try:
            log.trace("Executing lifecycle hook '{}' for action '{}'", hook_type, self.name)

            hook_action: ActionHook = HookFactory.load(hook_resource, self.context, self.deployment_details, self.name)

            # Hooks output details of these **kwargs given their Case or case.  I like Case capetalized.
            return hook_action.execute(State=hook_type, Reason=reason)

        except Exception as e:
            log.error(
                "Failed to execute lifecycle hook '{}' for action '{}': {}",
                hook_type,
                self.name,
                str(e),
            )
            return False
