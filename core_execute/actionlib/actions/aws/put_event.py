"""Record an event in the Core Execute database and log it."""

from typing import Any
from pydantic import Field, model_validator

import core_logging as log
import core_framework as util

from core_framework.models import ActionResource, DeploymentDetails, ActionSpec

from core_execute.actionlib.action import BaseAction

from core_db.event.actions import EventActions


class PutEventActionSpec(ActionSpec):
    """Parameters for PutEventAction.

    Attributes:
      type: Event type (STATUS, DEBUG, INFO, WARN, ERROR). Default: STATUS.
      status: Event status string (required).
      message: Optional message for the event. Default: "".
      identity: Optional event identity string.
    """

    type: str = Field(
        "STATUS",
        alias="Type",
        description="The type of event to put (STATUS, DEBUG, INFO, WARN, ERROR)",
    )
    status: str = Field(
        ...,
        alias="Status",
        description="The status of the event",
    )
    message: str = Field(
        "",
        alias="Message",
        description="The message associated with the event",
    )
    identity: str = Field(
        None,
        alias="Identity",
        description="The identity of the event",
    )

    @model_validator(mode="before")
    @classmethod
    def validatre_model_before(cls, values: Any) -> dict[str, Any]:
        """Provide default account/region to satisfy the base model."""
        if isinstance(values, dict):
            if not any(key in values for key in ["account", "Account"]):
                values["Account"] = "not-required"
            if not any(key in values for key in ["region", "Region"]):
                values["Region"] = "not-required"
        return values


class PutEventActionResource(ActionResource):
    """Resource model for PutEvent (forces kind and normalizes spec)."""

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Normalize incoming values and enforce canonical kind/spec."""
        if not isinstance(values, dict):
            return values

        values.pop("kind", None)
        values.pop("Kind", None)
        values["kind"] = "AWS::PutEvent"

        spec = values.pop("spec", None) or values.pop("Spec", None)
        if isinstance(spec, dict):
            values["spec"] = spec
        elif isinstance(spec, PutEventActionSpec):
            values["spec"] = spec.model_dump()

        return values


class PutEventAction(BaseAction):
    """Record an event in the DB and log at the appropriate level."""

    def __init__(
        self,
        definition: ActionResource,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        """Initialize the action and validate parameters.

        Args:
          definition: Action resource with metadata/spec.
          context: Rendering context for templates.
          deployment_details: Deployment metadata (scope used as item_type).

        """
        super().__init__(definition, context, deployment_details)

        # Validate the action parameters
        self.params = PutEventActionSpec(**definition.spec)

        self.item_type = deployment_details.scope

    def _execute(self):
        """Write the event to the DB and log using the selected type."""
        log.trace("PutEventAction._execute()")

        # Create a unique timestamp label for this event instance
        start_time = util.get_current_timestamp()
        datetime_label = start_time.replace(":", "-").replace(".", "-")  # Make filesystem/key safe

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
                raise ValueError("Invalid event type. Must be one of: STATUS, DEBUG, INFO, WARN, ERROR")

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
            self.set_state("message", f"Failed to save event to database: {error_message}")

            log.error("Failed to save event to database: {}", e)
            self.set_failed("Failed to save event to database")
            return

        log.trace("PutEventAction._execute() complete")

    def _check(self):
        """Not applicable; event recording is immediate."""
        log.trace("PutEventAction._check()")

        self.set_failed("Internal error - _check() should not have been called")

        log.trace("PutEventAction._check() complete")

    def _unexecute(self):
        """No rollback; events are permanent records."""
        pass

    def _cancel(self):
        """No-op; action completes immediately and cannot be cancelled."""
        pass

    def _resolve(self):
        """Render template variables in type, status, message, and identity."""
        log.trace("PutEventAction._resolve()")

        self.params.type = self.renderer.render_string(self.params.type, self.context)
        self.params.status = self.renderer.render_string(self.params.status, self.context)
        self.params.message = self.renderer.render_string(self.params.message, self.context)
        self.params.identity = self.renderer.render_string(self.params.identity, self.context)

        log.trace("PutEventAction._resolve() complete")

    @classmethod
    def generate_action_resource(cls, **kwargs) -> PutEventActionResource:
        """Factory: create a typed PutEventActionResource."""
        return PutEventActionResource(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> PutEventActionSpec:
        """Factory: create typed PutEventActionSpec."""
        return PutEventActionSpec(**kwargs)
