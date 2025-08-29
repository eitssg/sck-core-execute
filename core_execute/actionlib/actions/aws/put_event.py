"""Record an event in the database action for Core Execute automation platform."""

from typing import Any
from pydantic import Field, model_validator

import core_logging as log
import core_framework as util

from core_framework.models import ActionSpec, DeploymentDetails, ActionParams

from core_execute.actionlib.action import BaseAction

from core_db.event.actions import EventActions


class PutEventActionParams(ActionParams):
    """
    Parameters for the PutEventAction.

    Attributes
    ----------
    type : str
        The type of event to put. Valid values: STATUS, DEBUG, INFO, WARN, ERROR.
        Defaults to 'STATUS'.
    status : str
        The status of the event (required).
    message : str
        The message to associate with the event. Defaults to empty string.
    identity : str, optional
        The identity of the event. Defaults to None.
    """

    type: str = Field(
        "STATUS",
        alias="Type",
        description="The type of event to put (required) defaults to 'STATUS'",
    )
    status: str = Field(
        ...,
        alias="Status",
        description="The status of the event (required)",
    )
    message: str = Field(
        "",
        alias="Message",
        description="The message to associate with the event (optional) defaults to ''",
    )
    identity: str = Field(
        None,
        alias="Identity",
        description="The identity of the event (optional)",
    )

    @model_validator(mode="before")
    @classmethod
    def validatre_model_before(cls, values: Any) -> dict[str, Any]:
        if isinstance(values, dict):

            # These are requried keys in the superclass.  But, for this
            # Action, we'll just put 'non-required' in there
            if not any(key in values for key in ["account", "Account"]):
                values["Account"] = "not-required"
            if not any(key in values for key in ["region", "Region"]):
                values["Region"] = "not-required"

        return values


class PutEventActionSpec(ActionSpec):
    """
    Action specification for the PutEvent action.

    Provides validation and default values for PutEvent action definitions.
    """

    @model_validator(mode="before")
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """
        Validate and set default parameters for the PutEventActionSpec.

        :param values: Input values dictionary.
        :type values: dict[str, Any]
        :return: Validated values with defaults applied.
        :rtype: dict[str, Any]
        """
        if not (values.get("name") or values.get("Name")):
            values["name"] = "action-aws-putevent-name"
        if not (values.get("kind") or values.get("Kind")):
            values["kind"] = "AWS::PutEvent"
        if not values.get(
            "depends_on", values.get("DependsOn")
        ):  # arrays are falsy if empty
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Spec")):
            values["params"] = {
                "Type": "INFO",
                "Status": "",
                "Message": "",
                "Identity": None,
            }

        return values


class PutEventAction(BaseAction):
    """
    Record an event in the database.

    This action records an event in the database and outputs a log message
    based on the event type. The event will be associated with the deployment
    details and the identity of the event.

    The action supports different event types that determine both the logging
    level and the database record type.

    Attributes
    ----------
    params : PutEventActionParams
        Validated parameters for the action.

    Parameters
    ----------
    Kind : str
        Use the value: ``AWS::PutEvent``
    Spec.Type : str
        The type of event to put. Valid values: STATUS, DEBUG, INFO, WARN, ERROR.
        Defaults to 'STATUS'.
    Spec.Status : str
        The status of the event (required).
    Spec.Message : str
        The message to associate with the event. Defaults to empty string.
    Spec.Identity : str, optional
        The identity of the event. Defaults to None.

    Examples
    --------
    ActionSpec YAML configuration:

    .. code-block:: yaml

        - Name: action-aws-putevent-name
          Kind: "AWS::PutEvent"
          Spec:
            Type: "STATUS"
            Status: "DEPLOY_SUCCESS"
            Message: "The deployment was successful"
            Identity: "prn:stack-portfolio:my-stack-app:my-stack-dev-branch:ver.10"
          Scope: "build"

    Notes
    -----
    Event types map to specific logging levels:

    - STATUS: Uses log.status() with status and message
    - DEBUG: Uses log.debug() with message
    - INFO: Uses log.info() with message
    - WARN: Uses log.warn() with message
    - ERROR: Uses log.error() with message

    The event is always recorded in the database regardless of the logging level.

    Raises
    ------
    ValueError
        If an invalid event type is provided.
    """

    def __init__(
        self,
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        """
        Initialize the PutEventAction.

        :param definition: The action specification definition.
        :type definition: ActionSpec
        :param context: Execution context for variable resolution.
        :type context: dict[str, Any]
        :param deployment_details: Details about the current deployment.
        :type deployment_details: DeploymentDetails
        :raises ValidationError: If action parameters are invalid.
        """
        super().__init__(definition, context, deployment_details)

        # Validate the action parameters
        self.params = PutEventActionParams(**definition.params)

        self.item_type = deployment_details.scope

    def _execute(self):
        """
        Execute the event recording operation.

        Records the event in both the logging system and database.
        Sets appropriate completion or failure state based on the outcome.

        :raises ValueError: If an invalid event type is provided.
        """
        log.trace("PutEventAction._execute()")

        # Create a unique timestamp label for this event instance
        start_time = util.get_current_timestamp()
        datetime_label = start_time.replace(":", "-").replace(
            ".", "-"
        )  # Make filesystem/key safe

        # Track this event instance in general state
        self.set_state("last_event_time", start_time)
        self.set_state("last_event_type", self.params.type)
        self.set_state("last_event_status", self.params.status)
        self.set_state("total_events", self.get_state("total_events", 0) + 1)

        try:
            t = self.params.type.upper()
            if t == "STATUS":
                log.status(
                    self.params.status,
                    self.params.message,
                    identity=self.params.identity,
                )
            elif t == "DEBUG":
                log.debug(self.params.message, identity=self.params.identity)
            elif t == "INFO":
                log.info(self.params.message, identity=self.params.identity)
            elif t == "WARN":
                log.warn(self.params.message, identity=self.params.identity)
            elif t == "ERROR":
                log.error(self.params.message, identity=self.params.identity)
            else:
                log.fatal("Invalid event type: {}", t)
                raise ValueError(
                    f"Invalid event type: {t}.  Must be one of: STATUS, DEBUG, INFO, WARN, ERROR"
                )

            event = EventActions.create(
                self.params.identity,
                event_type=self.params.type,
                item_type=self.item_type,
                status=self.params.status,
                message=self.params.message,
            )
            log.debug("Event created: {}", event)

            # Set success state for this specific event instance
            events = self.get_state("events", {})
            completion_time = util.get_current_timestamp()
            events[completion_time] = {
                "type": self.params.type,
                "status": self.params.status,
                "message": self.params.message,
                "identity": self.params.identity,
            }
            # use set_output to respect the save_outputs flag
            self.set_output("events", events)

            self.set_complete("Event recorded successfully")

        except Exception as e:
            # Set error state information for this specific event instance
            error_time = util.get_current_timestamp()
            error_message = str(e)

            # Instance-specific error state
            # General error state (tracks last event attempt)
            self.set_state("last_event_time", start_time)
            self.set_state("last_event_type", self.params.type)
            self.set_state("last_event_status", "ERROR")
            self.set_state("last_error_message", error_message)
            self.set_state("status", "error")
            self.set_state("error_time", error_time)
            self.set_state("error_message", error_message)
            self.set_state(
                "message", f"Failed to save event to database: {error_message}"
            )

            log.error("Failed to save event to database: {}", e)
            self.set_failed("Failed to save event to database")
            return

        log.trace("PutEventAction._execute() complete")

    def _check(self):
        """
        Check the status of the event recording operation.

        This method should not be called for PutEvent actions as the
        operation completes immediately. If called, it indicates an internal error.
        """
        log.trace("PutEventAction._check()")

        self.set_failed("Internal error - _check() should not have been called")

        log.trace("PutEventAction._check() complete")

    def _unexecute(self):
        """
        Reverse the event recording operation.

        This operation cannot be reversed as events are permanent records.
        This method is provided for interface compliance but performs no action.
        """
        pass

    def _cancel(self):
        """
        Cancel the event recording operation.

        This operation cannot be cancelled as it completes immediately.
        This method is provided for interface compliance but performs no action.
        """
        pass

    def _resolve(self):
        """
        Resolve template variables in action parameters.

        Uses the renderer to substitute variables in the type, status, message,
        and identity parameters using the current execution context.
        """
        log.trace("PutEventAction._resolve()")

        self.params.type = self.renderer.render_string(self.params.type, self.context)
        self.params.status = self.renderer.render_string(
            self.params.status, self.context
        )
        self.params.message = self.renderer.render_string(
            self.params.message, self.context
        )
        self.params.identity = self.renderer.render_string(
            self.params.identity, self.context
        )

        log.trace("PutEventAction._resolve() complete")

    @classmethod
    def generate_action_spec(cls, **kwargs) -> PutEventActionSpec:
        return PutEventActionSpec(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> PutEventActionParams:
        return PutEventActionParams(**kwargs)
