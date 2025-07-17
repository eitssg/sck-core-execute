"""Defines the BasActions abstrction for all actions."""

from typing import Any, Self
import traceback
import sys
import os
import enum
import core_logging as log

from core_framework.models import ActionSpec, DeploymentDetails

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
    """Enum for action status codes."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETE = "complete"
    FAILED = "failed"


class BaseAction:
    """
    Base class for all actions in the Simple Cloud Kit execution framework.

    This abstract base class provides the foundation for implementing custom actions
    that can be executed as part of deployment workflows. All action subclasses must
    inherit from this class and implement the required abstract methods.

    The BaseAction class handles:

    - Action lifecycle management (pending, running, complete, failed states)
    - Jinja2 template rendering for dynamic configuration
    - Output and state variable management
    - Condition evaluation for conditional execution
    - Lifecycle hooks for status updates and notifications
    - Error handling and logging

    :param definition: The action specification containing configuration details
    :type definition: ActionSpec
    :param context: The Jinja2 rendering context containing all variables
    :type context: dict[str, Any]
    :param deployment_details: Client/portfolio/app/branch/build information
    :type deployment_details: DeploymentDetails

    :ivar name: The unique identifier of the action (e.g., "client:action/deploy-stack")
    :vartype name: str
    :ivar context: The Jinja2 rendering context containing all variables
    :vartype context: dict[str, Any]
    :ivar action_name: The short name of the action (e.g., "deploy-stack")
    :vartype action_name: str
    :ivar output_namespace: Namespace for saved outputs (e.g., "client:output")
    :vartype output_namespace: str | None
    :ivar state_namespace: Namespace for state variables (e.g., "client:var")
    :vartype state_namespace: str
    :ivar kind: The type/kind of action (e.g., "CloudFormation", "Lambda")
    :vartype kind: str
    :ivar condition: Jinja2 condition that determines if action should execute
    :vartype condition: str
    :ivar after: List of actions that must complete before this action
    :vartype after: list[str]
    :ivar lifecycle_hooks: Hooks for status notifications
    :vartype lifecycle_hooks: list[dict[str, Any]]
    :ivar deployment_details: Client/portfolio/app/branch/build info
    :vartype deployment_details: DeploymentDetails
    :ivar renderer: Template renderer using the action's context
    :vartype renderer: Jinja2Renderer

    .. note::
        All underscore-prefixed methods (_execute, _check, _resolve, _cancel, _unexecute)
        must be implemented in subclasses.

    Example:
        Creating a custom action subclass:

        .. code-block:: python

            class MyCustomAction(BaseAction):
                def _resolve(self):
                    # Resolve any dependencies or prepare for execution
                    self.set_state("resolved", True)

                def _execute(self):
                    # Main action logic
                    self.set_running("Executing custom logic")

                    try:
                        # Your custom implementation here
                        result = self.do_something()
                        self.set_output("result", result)
                        self.set_complete("Custom action completed successfully")
                    except Exception as e:
                        self.set_failed(f"Custom action failed: {e}")

                def _check(self):
                    # Check if action can run (e.g., validate prerequisites)
                    if not self.validate_prerequisites():
                        self.set_failed("Prerequisites not met")

                def _cancel(self):
                    # Cancel a running action
                    self.cleanup_resources()
                    self.set_failed("Action was cancelled")

                def _unexecute(self):
                    # Rollback/undo the action
                    self.rollback_changes()
                    self.set_complete("Action rolled back")

    .. rubric:: Implementation Guidelines

    - Use self.set_running(), self.set_complete(), self.set_failed() to manage state
    - Use self.set_output() to save results that other actions can reference
    - Use self.set_state() for internal state management
    - Use self.renderer.render_string() to process Jinja2 templates
    - Access deployment context via self.context dictionary
    - The execute() and check() methods are final and handle error handling/logging

    .. rubric:: State Management

    Actions progress through states: PENDING -> RUNNING -> COMPLETE/FAILED

    - **PENDING**: Initial state, action not yet started
    - **RUNNING**: Action is currently executing
    - **COMPLETE**: Action finished successfully
    - **FAILED**: Action encountered an error

    .. rubric:: Context Variables

    The context dictionary contains all variables available for Jinja2 rendering:

    - Deployment details (client, portfolio, app, branch, build)
    - Outputs from previous actions
    - State variables from all actions
    - Environment variables and configuration

    .. seealso::
        :class:`ActionSpec`: Defines the action configuration
        :class:`DeploymentDetails`: Contains deployment metadata
        :class:`Jinja2Renderer`: Handles template rendering
    """

    name: str
    """str: The name of the action is the unique identifier of the action."""

    context: dict[str, Any]
    """dict[str, Any]: The context of the action is the Jinja2 Rendering Context."""

    action_name: str
    """str: The action name is the name of the action."""

    output_namespace: str | None
    """str | None: The output namespace is the namespace where the output of the action is stored."""

    state_namespace: str
    """str: The state namespace is the namespace where the state of the action is stored."""

    kind: str
    """str: The kind of the action."""

    condition: str
    """str: The condition of the action."""

    after: list[str]
    """list[str]: The actions that should be perfomed after this action."""

    lifecycle_hooks: list[dict[str, Any]]
    """list[dict[str, Any]]: The lifecycle hooks of the action."""

    deployment_details: DeploymentDetails
    """DeploymentDetails: The deployment details of the action. client/portfolio/app/branch/build information."""

    renderer: Jinja2Renderer
    """Jinja2Renderer: The Jinja2 Renderer for the action.  Uses the context to render templates."""

    def _execute(self):
        raise NotImplementedError("Must implement in subclass")

    def _check(self):
        raise NotImplementedError("Must implement in subclass")

    def _resolve(self):
        raise NotImplementedError("Must implement in subclass")

    def _cancel(self):
        raise NotImplementedError("Must implement in subclass")

    def _unexecute(self):
        raise NotImplementedError("Must implement in subclass")

    def __init__(
        self,
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):

        log.trace("BaseAction.__init__()")

        # All actions can use the Jinja2 renderer to parse CloudFormation.j2 templates
        self.renderer = Jinja2Renderer()

        # Extract action details from the definition
        self.name = definition.name

        log.debug("Action name is: {}", self.name)

        self.context = context

        log.debug("Action context is: ", details=self.context)

        self.deployment_details = deployment_details

        self.action_name = self.name.split("/", 1)[-1]

        # Set output_namespace if user specified SaveOutputs = True
        if definition.save_outputs:
            self.output_namespace = self.name.split("/", 1)[0].replace(":action", ":output")
        else:
            self.output_namespace = None

        log.debug("Action output namespace is {}", self.output_namespace)

        # State namespace is the same as action name, except with :var/ instead of :action/
        self.state_namespace = self.name.replace(":action/", ":var/")

        log.debug("Action state namespace is {}", self.state_namespace)

        after = definition.after or []
        depends = definition.depends_on or []

        self.kind = definition.kind
        self.condition = definition.condition or "True"
        self.before = definition.before or []
        self.after = after + depends
        self.lifecycle_hooks = definition.lifecycle_hooks or []

        log.trace("BaseAction.__init__() - complete")

    def is_init(self) -> bool:
        """
        Check if the action is in the init state.

        :return: True if the action is in the init state
        :rtype: bool
        """
        return self.__get_status_code() == StatusCode.PENDING.value

    def is_failed(self) -> bool:
        """
        Check if the action is in the failed state.

        :return: True if the action is in the failed state
        :rtype: bool
        """
        return self.__get_status_code() == StatusCode.FAILED.value

    def is_running(self) -> bool:
        """
        Check if the action is in the running state.

        :return: True if the action is in the running state
        :rtype: bool
        """
        return self.__get_status_code() == StatusCode.RUNNING.value

    def is_complete(self) -> bool:
        """
        Check if the action is in the complete state.

        :return: True if the action is in the complete state
        :rtype: bool
        """
        return self.__get_status_code() == StatusCode.COMPLETE.value

    def set_failed(self, reason: str):
        """
        Set the status to failed and supply the given reason.

        :param reason: Reason the status failed
        :type reason: str

        .. note::
            This method will ignore duplicate state updates with the same reason.
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
        """
        Set the status to running and supply the given reason.

        :param reason: The reason the status is running
        :type reason: str

        .. note::
            This method will ignore duplicate state updates with the same reason.
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
        """
        Set the status to complete and supply the given reason.

        :param reason: The reason the status is complete. Defaults to "Action finished."
        :type reason: str | None

        .. note::
            This method will ignore duplicate state updates with the same reason.
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
        """
        Set the status to skipped and supply the given reason.

        :param reason: The reason the status is skipped
        :type reason: str

        .. note::
            This method will ignore duplicate state updates with the same reason.
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
        """
        Set the output of the action.

        :param name: Variable name of the output within the action output namespace
        :type name: str
        :param value: Value of this variable
        :type value: Any

        .. note::
            If SaveOutputs is enabled, the output will be saved to both the output
            namespace and the state namespace. Otherwise, only the state namespace
            is used.
        """
        log.trace("Setting output '{}' = '{}'", name, value)

        # Set output variable (if user chose to save outputs)
        if self.output_namespace is not None:
            log.debug("Setting output '{}/{}' = '{}'", self.output_namespace, name, value)
            self.__set_context(self.output_namespace, name, value)

        # Set state variable
        self.__set_context(self.state_namespace, name, value)

        log.trace("Output '{}' set to '{}'", name, value)

    def get_output(self, name: str, default: Any = NO_DEFAULT_PROVIDED) -> str | None:
        """
        Get the output variable from the action within the output namespace.

        :param name: Name of the output variable
        :type name: str
        :return: Value of the output variable, or None if not found or no output namespace
        :rtype: str | None
        """
        log.trace("Getting output '{}'", name)

        if self.output_namespace is None:
            return None
        return self.__get_context(self.output_namespace, name, default)

    def set_state(self, name: str, value: Any):
        """
        Set a state variable of the action within the action state namespace.

        :param name: Name of the state variable
        :type name: str
        :param value: Value of the state variable
        :type value: Any
        """
        log.trace("Setting state '{}' = '{}'", name, value)

        self.__set_context(self.state_namespace, name, value)

    def get_state(self, name: str) -> str:
        """
        Get a state variable of the action within the action state namespace.

        :param name: Name of the state variable
        :type name: str
        :return: Value of the state variable
        :rtype: str
        :raises KeyError: If the state variable is not found
        """
        log.trace("Getting state '{}'", name)

        return self.__get_context(self.state_namespace, name)

    def execute(self) -> Self:
        """
        Execute the action after checking its condition.

        This method first evaluates the action's condition using Jinja2 templating.
        If the condition evaluates to true, the action is resolved and executed.
        If false, the action is skipped.

        The context is used to render action variables and parameters before execution.

        :return: This action object for method chaining
        :rtype: Self

        .. note::
            This method is final and handles error handling and logging automatically.
            Subclasses should implement _resolve() and _execute() instead.

        .. warning::
            Any exceptions raised during execution will be caught and the action
            will be set to failed status with detailed error information.
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
        """
        Check the action to determine if it is ready to run.

        This method resolves dependencies and performs pre-execution checks
        to validate that the action can be executed successfully.

        :return: This action object for method chaining
        :rtype: Self

        .. note::
            This method is final and handles error handling and logging automatically.
            Subclasses should implement _resolve() and _check() instead.

        .. warning::
            Any exceptions raised during checking will be caught and the action
            will be set to failed status with detailed error information.
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

    def __get_status_code(self):
        return self.__get_context(self.name, STATUS_CODE, StatusCode.PENDING.value)

    def __get_status_reason(self):
        return self.__get_context(self.name, STATUS_REASON, None)

    def __get_context(self, prn: str, name: str, default: str = NO_DEFAULT_PROVIDED) -> str:
        key = "{}/{}".format(prn, name)

        if self.context and key in self.context:
            return self.context[key]

        else:
            if default == NO_DEFAULT_PROVIDED:
                raise KeyError("Key '{}' is not in the context and no default was provided".format(name))
            else:
                return default

    def __set_context(self, prn: str, name: str, value: Any):

        key = "{}/{}".format(prn, name)

        self.context[key] = value

    def __execute_lifecycle_hooks(self, event: str, reason: str):
        # Retrieve the event hooks for this action, for this state event
        event_hooks = [h for h in self.lifecycle_hooks if event in h.get("States", [])]

        # Execute the event hooks
        for event_hook in event_hooks:
            hook_type = event_hook["Type"]
            self.__execute_lifecycle_hook(event, hook_type, event_hook, reason)

    def __execute_lifecycle_hook(self, event: str, hook_type: str, hook: dict[str, Any], reason: str):
        if hook_type == LC_TYPE_STATUS:
            self.__execute_status_hook(event, hook, reason)
        else:
            raise Exception("Unsupported hook type {}".format(hook_type))

    def __get_status_parameter(self, event: str, hook: dict[str, Any]) -> str | None:
        key = f"On{event}"
        parms = hook.get("Parameters", {})
        if key in parms:
            action = parms[key]
            if "Status" in action:
                return action["Status"]
        return None

    def __get_message_parameter(self, event, hook: dict[str, Any]) -> str | None:
        key = f"On{event}"
        parms = hook.get("Parameters", {})
        if key in parms:
            action = parms[key]
            if "Message" in action:
                return action["Message"]
        return None

    def __get_idenity_parameter(self, event: str, hook: dict[str, Any]) -> str | None:
        parms = hook.get("Parameters", hook)
        if "Identity" in parms:
            return parms["Identity"]
        return None

    def __get_details_parameter(self, event: str, hook: dict[str, Any]) -> dict | None:
        parms = hook.get("Parameters", hook)
        if "Details" in parms:
            return parms["Details"]
        return None

    def __update_item_status(self, identity: str, status: str, message: str, details: Any):
        try:
            # Log the status
            log.set_identity(identity)

            prn_sections = identity.split(":")

            # Build PRN
            if len(prn_sections) == 5:
                build_prn = ":".join(prn_sections[0:5])

                # Update the build status
                update_status(prn=build_prn, status=status, message=message, details=details)

                # If a new build is being released, update the branch's released_build_prn pointer
                if status == RELEASE_IN_PROGRESS:
                    branch_prn = ":".join(prn_sections[0:4])
                    update_item(prn=branch_prn, released_build_prn=build_prn)

            # Component PRN
            if len(prn_sections) == 6:
                component_prn = ":".join(prn_sections[0:6])

                # Update the component status
                update_status(prn=component_prn, status=status, message=message, details=details)

                # If component has failed, update the build status to failed
                if "_FAILED" in status:
                    build_prn = ":".join(prn_sections[0:5])

                    # Update the build status
                    update_status(prn=build_prn, status=status)

        except Exception as e:
            log.warn("Failed to update status via API - {}", e)

        finally:
            log.reset_identity()

    def __execute_status_hook(self, event: str, hook: dict[str, Any], reason: str | None):

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
        return "{}({})".format(type(self).__name__, self.name)

    def __str__(self):
        return "{}({})".format(type(self).__name__, self.name)
