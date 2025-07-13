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


ACT_LABEL = "Label"
ACT_TYPE = "Type"
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
    """BaseActions the class where all actions inherit from."""

    label: str
    """str: The label of the action is the unique identifier of the action."""

    context: dict[str, Any]
    """dict[str, Any]: The context of the action is the Jinja2 Rendering Context."""

    action_name: str
    """str: The action name is the name of the action."""

    output_namespace: str | None
    """str | None: The output namespace is the namespace where the output of the action is stored."""

    state_namespace: str
    """str: The state namespace is the namespace where the state of the action is stored."""
    type: str
    """str: The type of the action."""

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
        self.label = definition.label

        log.debug("Action label is: {}", self.label)

        self.context = context

        log.debug("Action context is: ", details=self.context)

        self.deployment_details = deployment_details

        self.action_name = self.label.split("/", 1)[-1]

        # Set output_namespace if user specified SaveOutputs = True
        if definition.save_outputs:
            self.output_namespace = self.label.split("/", 1)[0].replace(
                ":action", ":output"
            )
        else:
            self.output_namespace = None

        log.debug("Action output namespace is {}", self.output_namespace)

        # State namespace is the same as action label, except with :var/ instead of :action/
        self.state_namespace = self.label.replace(":action/", ":var/")

        log.debug("Action state namespace is {}", self.state_namespace)

        after = definition.after or []
        depends = definition.depends_on or []

        self.type = definition.type
        self.condition = definition.condition or "True"
        self.before = definition.before or []
        self.after = after + depends
        self.lifecycle_hooks = definition.lifecycle_hooks or []

        log.trace("BaseAction.__init__() - complete")

    def is_init(self) -> bool:
        """
        Check if the action is in the init state.

        Returns:
            bool: True if the action is in the init state
        """
        return self.__get_status_code() == StatusCode.PENDING.value

    def is_failed(self) -> bool:
        """
        Check if the action is in the failed state.

        Returns:
            bool: True if the action is in the failed state
        """
        return self.__get_status_code() == StatusCode.FAILED.value

    def is_running(self) -> bool:
        """
        Check if the action is in the running state.

        Returns:
            bool: True if the action is in the running state
        """
        return self.__get_status_code() == StatusCode.RUNNING.value

    def is_complete(self) -> bool:
        """
        Check if the action is in the complete state.

        Returns:
            bool: True if the action is in the complete state
        """
        return self.__get_status_code() == StatusCode.COMPLETE.value

    def set_failed(self, reason: str):
        """
        Set the status to failed and supply the given reason

        Args:
            reason (str): Reason the status failed.
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
        self.__set_context(self.label, STATUS_CODE, StatusCode.FAILED.value)
        self.__set_context(self.label, STATUS_REASON, reason)

        log.trace("Action set to failed - {}", reason)

    def set_running(self, reason: str):
        """
        Set the status to running and supply the given reason

        Args:
            reason (str): The reason the status is running.
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
        self.__set_context(self.label, STATUS_CODE, StatusCode.RUNNING.value)
        self.__set_context(self.label, STATUS_REASON, reason)

        log.trace("Action set to running - {}", reason)

    def set_complete(self, reason: str | None = None):
        """
        Set the status to complete and supply the given reason

        Args:
            reason (str): The reason the status is complete.
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
        self.__set_context(self.label, STATUS_CODE, StatusCode.COMPLETE.value)
        self.__set_context(self.label, STATUS_REASON, reason)

        log.trace("Action set to complete - {}", reason)

    def set_skipped(self, reason: str):
        """
        Set the status to skipped and supply the given reason

        Args:
            reason (str): The reason the status is skipped.
        """
        log.trace("Setting action to skipped - {}", reason)

        # Ignore duplicate state updates
        if self.is_complete() and self.__get_status_reason() == reason:
            log.trace("Action is already complete - {}", reason)
            return

        # Log the state change
        log.debug("Action has been skipped - {}", reason)

        # Update the context with the new state
        self.__set_context(self.label, STATUS_CODE, StatusCode.COMPLETE.value)
        self.__set_context(self.label, STATUS_REASON, reason)

        log.trace("Action set to skipped - {}", reason)

    def set_output(self, name: str, value: Any):
        """
        Set the output of the action

        Args:
            name (str): Variable name of the output within the action output namespace.
            value (Any): Value of this variable
        """
        log.trace("Setting output '{}' = '{}'", name, value)

        # Set output variable (if user chose to save outputs)
        if self.output_namespace is not None:
            log.debug(
                "Setting output '{}/{}' = '{}'", self.output_namespace, name, value
            )
            self.__set_context(self.output_namespace, name, value)

        # Set state variable
        self.__set_context(self.state_namespace, name, value)

        log.trace("Output '{}' set to '{}'", name, value)

    def get_output(self, name: str) -> str | None:
        """
        Get the output variable from the action within the output namespace

        Args:
            name (str): Name of the output variable

        Returns:
            str | None: Value of the output variable
        """

        log.trace("Getting output '{}'", name)

        if self.output_namespace is None:
            return None
        return self.__get_context(self.output_namespace, name)

    def set_state(self, name: str, value: Any):
        """
        Set a state variable of the action within the action state namespace

        Args:
            name (str): Name of the state variable
            value (Any): vaue of the state variable
        """
        log.trace("Setting state '{}' = '{}'", name, value)

        self.__set_context(self.state_namespace, name, value)

    def get_state(self, name: str) -> str:
        """
        Get a state variable of the action within the action state namespace

        Args:
            name (str): Name of the state variable

        Returns:
            str: Value of the state variable
        """

        log.trace("Getting state '{}'", name)

        return self.__get_context(self.state_namespace, name)

    def execute(self) -> Self:
        """
        Execute the action.  It will first check the condition, and if true, execute the action.

        The context is used to render action variables and paramters and the action is executed.

        Returns:
            Self: This action object
        """

        try:
            # Temporarily set the logger identity to this action's label
            log.set_identity(self.label)

            log.trace("Executing action for {}", self.label)

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

            log.trace("Action executed for {}", self.label)

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
        """
        Check the action to determine if it is oc to run

        Returns:
            Self: _description_
        """

        try:
            # Temporarily set the logger identity to this action's label
            log.set_identity(self.label)

            log.debug("Checking action for {}", self.label)

            self._resolve()
            self._check()

            log.trace("Action checked for {}", self.label)

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
        return self.__get_context(self.label, STATUS_CODE, StatusCode.PENDING.value)

    def __get_status_reason(self):
        return self.__get_context(self.label, STATUS_REASON, None)

    def __get_context(
        self, prn: str, name: str, default: str = NO_DEFAULT_PROVIDED
    ) -> str:
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

        key = "{}/{}".format(prn, name)

        self.context[key] = value

    def __execute_lifecycle_hooks(self, event: str, reason: str):
        # Retrieve the event hooks for this action, for this state event
        event_hooks = [h for h in self.lifecycle_hooks if event in h.get("States", [])]

        # Execute the event hooks
        for event_hook in event_hooks:
            hook_type = event_hook["Type"]
            self.__execute_lifecycle_hook(event, hook_type, event_hook, reason)

    def __execute_lifecycle_hook(
        self, event: str, hook_type: str, hook: dict[str, Any], reason: str
    ):
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

    def __update_item_status(
        self, identity: str, status: str, message: str, details: Any
    ):
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
        return "{}({})".format(type(self).__name__, self.label)

    def __str__(self):
        return "{}({})".format(type(self).__name__, self.label)
