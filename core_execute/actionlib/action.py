"""Defines the BaseActions abstraction for all actions."""

from typing import Any, Self, Optional
import traceback
import sys
import os
import enum
import core_logging as log

from core_framework.models import (
    ActionSpec,
    ActionMetadata,
    ActionParams,
    DeploymentDetails,
)

from core_renderer import Jinja2Renderer

from core_framework.status import RELEASE_IN_PROGRESS

from core_db.dbhelper import update_status, update_item


ACT_NAME = "Name"
ACT_KIND = "Kind"
ACT_CONDITION = "Condition"
ACT_BEFORE = "Before"
ACT_AFTER = "After"
ACT_LIFECYCLE_HOOKS = "LifecycleHooks"
ACT_SAVE_OUTPUTS = "SaveOutputs"
ACT_DEPENDS_ON = "DependsOn"
ACT_STATUS_HOOOK = "StatusHook"

STATUS_CODE = "StatusCode"
STATUS_REASON = "StatusReason"

LC_TYPE_STATUS = "status"
LC_HOOK_PENDING = "Pending"
LC_HOOK_FAILED = "Failed"
LC_HOOK_RUNNING = "Running"
LC_HOOK_COMPLETE = "Complete"

NO_DEFAULT_PROVIDED = "_!NO!DEFAULT!PROVIDED!_"


class StatusCode(enum.Enum):
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


class BaseAction(object):
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
        definition (ActionSpec): ActionSpec specification from deployspec.yaml
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

    lifecycle_hooks: list[dict[str, Any]]
    """Status notification hooks for action state changes"""

    deployment_details: DeploymentDetails
    """Deployment metadata (client, portfolio, app, branch, build)"""

    renderer: Jinja2Renderer
    """Template renderer using the action's context for variable substitution"""

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
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        """Initialize a new BaseAction instance.

        Sets up the action with configuration from the ActionSpec, initializes
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

        # Handle metadata-based vs legacy name-based configuration
        self._resolve_action_identity()

        log.debug("Action name is: {}", self.name)
        log.debug("Action output namespace is: {}", self.output_namespace)
        log.debug("Action state namespace is: {}", self.state_namespace)
        log.debug("Action context is: ", details=self.context)

        after = definition.after or []
        depends = definition.depends_on or []

        self.kind = definition.kind
        self.condition = definition.condition or "True"
        self.before = definition.before or []
        self.after = after + depends
        self.lifecycle_hooks = definition.lifecycle_hooks or []

        log.trace("BaseAction.__init__() - complete")

    def _resolve_action_identity(self):
        """Resolve action identity using metadata-first approach with legacy fallback.

        Priority order:
        1. Use metadata.name and metadata.namespace if available (modern approach)
        2. Fall back to legacy name field parsing if metadata not available
        3. Create metadata from legacy name for forward compatibility

        Raises:
            ValueError: If neither metadata.name nor legacy name field is provided
        """
        # Check if we have modern metadata structure
        if self.definition.metadata and self.definition.metadata.name:
            # Modern metadata-based approach
            self._setup_from_metadata()
            log.debug("Using metadata-based action identity")

        elif self.definition.name:
            # Legacy name-based approach with metadata creation
            self._setup_from_legacy_name()
            log.debug("Using legacy name-based action identity with metadata creation")

        else:
            raise ValueError(
                "Action must have either metadata.name or legacy name field"
            )

    def _setup_from_metadata(self):
        """Setup action identity using metadata.name and metadata.namespace.

        Configures action name, namespaces, and output settings based on the
        ActionMetadata structure. Transforms namespaces for output and state
        variable organization.
        """
        metadata = self.definition.metadata

        # Set basic properties from metadata
        self.action_name = metadata.name

        # Handle save_outputs - check both definition level and metadata
        save_outputs = getattr(self.definition, "save_outputs", None)
        if save_outputs is None:
            save_outputs = (
                metadata.save_outputs if metadata.save_outputs is not None else True
            )
        self.save_outputs = save_outputs

        # Build full name for backwards compatibility
        if metadata.namespace:
            self.name = f"{metadata.namespace}/{metadata.name}"
        else:
            self.name = metadata.name

        # Calculate namespaces
        if metadata.namespace:
            # Use explicit namespace from metadata
            base_namespace = metadata.namespace

            # Transform namespace for outputs (e.g., "myapp:action" -> "myapp:output")
            if ":action" in base_namespace:
                self.output_namespace = (
                    base_namespace.replace(":action", ":output")
                    if save_outputs
                    else None
                )
                self.state_namespace = base_namespace.replace(":action", ":var")
            else:
                # For simple namespaces, append type suffixes
                self.output_namespace = (
                    f"{base_namespace}:output" if save_outputs else None
                )
                self.state_namespace = f"{base_namespace}:var"
        else:
            # No namespace provided, use action name as namespace
            self.output_namespace = self.action_name if save_outputs else None
            self.state_namespace = self.action_name

    def _setup_from_legacy_name(self):
        """Setup action identity using legacy name field and create metadata.

        Parses the legacy name field to extract namespace and action name,
        then creates ActionMetadata for forward compatibility. Maintains
        backward compatibility with existing action configurations.
        """
        legacy_name = self.definition.name
        self.name = legacy_name

        # Parse legacy name to extract action_name and namespace
        if "/" in legacy_name:
            parts = legacy_name.split("/")
            namespace_part = parts[0]
            action_name_part = parts[-1]
        else:
            namespace_part = None
            action_name_part = legacy_name

        self.action_name = action_name_part

        # Handle save_outputs
        save_outputs = getattr(self.definition, "save_outputs", None)
        if save_outputs is None:
            save_outputs = True  # Default True for backwards compatibility
        self.save_outputs = save_outputs

        # Calculate namespaces using legacy logic
        if namespace_part:
            # Transform namespace for outputs (e.g., "myapp:action" -> "myapp:output")
            if ":action" in namespace_part:
                self.output_namespace = (
                    namespace_part.replace(":action", ":output")
                    if save_outputs
                    else None
                )
                self.state_namespace = legacy_name.replace(":action/", ":var/")
            else:
                # For simple namespaces
                self.output_namespace = namespace_part if save_outputs else None
                self.state_namespace = legacy_name
        else:
            # No namespace in legacy name
            self.output_namespace = legacy_name if save_outputs else None
            self.state_namespace = legacy_name

        # Create metadata from legacy name for forward compatibility
        self._create_metadata_from_legacy_name(namespace_part, action_name_part)

    def _create_metadata_from_legacy_name(
        self, namespace: str | None, action_name: str
    ):
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

    def is_init(self) -> bool:
        """Check if the action is in the initial pending state.

        Returns:
            True if the action has not started execution
        """
        return self.__get_status_code() == StatusCode.PENDING.value

    def is_failed(self) -> bool:
        """Check if the action is in the failed state.

        Returns:
            True if the action encountered an error during execution
        """
        return self.__get_status_code() == StatusCode.FAILED.value

    def is_running(self) -> bool:
        """Check if the action is currently executing.

        Returns:
            True if the action is currently running
        """
        return self.__get_status_code() == StatusCode.RUNNING.value

    def is_complete(self) -> bool:
        """Check if the action completed successfully.

        Returns:
            True if the action finished execution without errors
        """
        return self.__get_status_code() == StatusCode.COMPLETE.value

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
        self.__execute_lifecycle_hooks(LC_HOOK_FAILED, reason)

        # Update the context with the new state
        self.__set_context(self.name, STATUS_CODE, StatusCode.FAILED.value)
        self.__set_context(self.name, STATUS_REASON, reason)

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
        self.__execute_lifecycle_hooks(LC_HOOK_RUNNING, reason)

        # Update the context with the new state
        self.__set_context(self.name, STATUS_CODE, StatusCode.RUNNING.value)
        self.__set_context(self.name, STATUS_REASON, reason)

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
        self.__execute_lifecycle_hooks(LC_HOOK_COMPLETE, reason)

        # Update the context with the new state
        self.__set_context(self.name, STATUS_CODE, StatusCode.COMPLETE.value)
        self.__set_context(self.name, STATUS_REASON, reason)

        log.trace("Action set to complete - {}", reason)

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

        # Update the context with the new state
        self.__set_context(self.name, STATUS_CODE, StatusCode.COMPLETE.value)
        self.__set_context(self.name, STATUS_REASON, reason)

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
            log.debug(
                "Setting output '{}/{}' = '{}'", self.output_namespace, name, value
            )
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
            condition_result = self.renderer.render_string(
                "{{ " + self.condition + " }}", self.context
            )

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
                "Internal error {} in {} at {} - {}\nTraceback:\n{}".format(
                    exc_type.__name__, fname, lineno, str(e), tb_str
                )
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
                "Internal error {} in {} at {} - {}\nTraceback:\n{}".format(
                    exc_type.__name__, fname, lineno, str(e), tb_str
                )
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

    def __get_status_code(self):
        """Get the current status code from context."""
        return self.__get_context(self.name, STATUS_CODE, StatusCode.PENDING.value)

    def __get_status_reason(self):
        """Get the current status reason from context."""
        return self.__get_context(self.name, STATUS_REASON, None)

    def __get_context(
        self, prn: str, name: str, default: Any = NO_DEFAULT_PROVIDED
    ) -> Any:
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
                raise KeyError(
                    "Key '{}' is not in the context and no default was provided".format(
                        name
                    )
                )
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

    def __execute_lifecycle_hooks(self, event: str, reason: str):
        """Execute lifecycle hooks for the specified event.

        Args:
            event: Lifecycle event (Pending, Running, Complete, Failed)
            reason: Reason for the state change
        """
        # Retrieve the event hooks for this action, for this state event
        event_hooks = [h for h in self.lifecycle_hooks if event in h.get("States", [])]

        # Execute the event hooks
        for event_hook in event_hooks:
            hook_type = event_hook["Type"]
            self.__execute_lifecycle_hook(event, hook_type, event_hook, reason)

    def __execute_lifecycle_hook(
        self, event: str, hook_type: str, hook: dict[str, Any], reason: str
    ):
        """Execute a single lifecycle hook.

        Args:
            event: Lifecycle event name
            hook_type: Type of hook to execute
            hook: Hook configuration
            reason: Reason for the state change

        Raises:
            Exception: If unsupported hook type is encountered
        """
        if hook_type == LC_TYPE_STATUS:
            self.__execute_status_hook(event, hook, reason)
        else:
            raise Exception("Unsupported hook type {}".format(hook_type))

    def __get_status_parameter(self, event: str, hook: dict[str, Any]) -> str | None:
        """Extract status parameter from lifecycle hook configuration."""
        key = f"On{event}"
        parms = hook.get("Parameters", {})
        if key in parms:
            action = parms[key]
            if "Status" in action:
                return action["Status"]
        return None

    def __get_message_parameter(self, event, hook: dict[str, Any]) -> str | None:
        """Extract message parameter from lifecycle hook configuration."""
        key = f"On{event}"
        parms = hook.get("Parameters", {})
        if key in parms:
            action = parms[key]
            if "Message" in action:
                return action["Message"]
        return None

    def __get_idenity_parameter(self, event: str, hook: dict[str, Any]) -> str | None:
        """Extract identity parameter from lifecycle hook configuration."""
        parms = hook.get("Parameters", hook)
        if "Identity" in parms:
            return parms["Identity"]
        return None

    def __get_details_parameter(self, event: str, hook: dict[str, Any]) -> dict | None:
        """Extract details parameter from lifecycle hook configuration."""
        parms = hook.get("Parameters", hook)
        if "Details" in parms:
            return parms["Details"]
        return None

    def __update_item_status(
        self, identity: str, status: str, message: str, details: Any
    ):
        """Update status in the database for the specified identity.

        Args:
            identity: PRN identity to update
            status: New status value
            message: Status message
            details: Additional status details
        """
        try:
            # Log the status
            log.set_identity(identity)

            prn_sections = identity.split(":")

            # Build PRN
            if len(prn_sections) == 5:
                build_prn = ":".join(prn_sections[0:5])

                # Update the build status
                update_status(
                    prn=build_prn, status=status, message=message, details=details
                )

                # If a new build is being released, update the branch's released_build_prn pointer
                if status == RELEASE_IN_PROGRESS:
                    branch_prn = ":".join(prn_sections[0:4])
                    update_item(prn=branch_prn, released_build_prn=build_prn)

            # Component PRN
            if len(prn_sections) == 6:
                component_prn = ":".join(prn_sections[0:6])

                # Update the component status
                update_status(
                    prn=component_prn, status=status, message=message, details=details
                )

                # If component has failed, update the build status to failed
                if "_FAILED" in status:
                    build_prn = ":".join(prn_sections[0:5])

                    # Update the build status
                    update_status(prn=build_prn, status=status)

        except Exception as e:
            log.warn("Failed to update status via API - {}", e)

        finally:
            log.reset_identity()

    def __execute_status_hook(
        self, event: str, hook: dict[str, Any], reason: str | None
    ):
        """Execute a status lifecycle hook.

        Args:
            event: Lifecycle event name
            hook: Hook configuration
            reason: Reason for the state change
        """
        # Extract hook["Parameter"]["On<event>"]["Status"], then try hook["Status"]
        status = self.__get_status_parameter(event, hook)
        message = self.__get_message_parameter(event, hook)
        identity = self.__get_idenity_parameter(event, hook)
        details = self.__get_details_parameter(event, hook)

        # Render templated parameters if a template has been provided
        if status:
            status = self.renderer.render_string(status, self.context)
        if identity:
            identity = self.renderer.render_string(identity, self.context)
        if message:
            message = self.renderer.render_string(message, self.context)

        # Ensure a status was provided
        if not status:
            log.warn(
                "Internal - status hook was executed, but no status was defined for event",
                details={ACT_STATUS_HOOOK: hook},
            )
            return

        # Ensure the identity was provided
        if not identity:
            log.warn(
                "Internal - status hook was executed, but no identity was defined",
                details={ACT_STATUS_HOOOK: hook},
            )
            return

        # Append reason to the message
        if reason:
            message = f"{message} - {reason}" if message else reason

        # Still no message?  Then see if we can finally get one set.
        if not message:
            message = reason if reason else ""

        # Update the status of the item
        self.__update_item_status(identity, status, message, details)

    def __repr__(self):
        """String representation for debugging."""
        return "{}({})".format(type(self).__name__, self.name)

    def __str__(self):
        """String representation for display."""
        return "{}({})".format(type(self).__name__, self.name)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> ActionParams:
        """Generate validated action parameters for this action type.

        Subclasses should override this to return a validated parameter set
        specific to their action type.

        Args:
            **kwargs: Parameter values to validate

        Returns:
            Validated ActionParams instance
        """
        return ActionParams(**kwargs)

    @classmethod
    def generate_action_spec(cls, **kwargs) -> ActionSpec:
        """Generate an ActionSpec for this action type.

        Subclasses should override this to return an ActionSpec with
        appropriate defaults and validation for their specific action type.

        Args:
            **kwargs: ActionSpec values to override

        Returns:
            ActionSpec instance for this action type
        """
        return ActionSpec(**kwargs)
