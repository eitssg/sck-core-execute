"""Status lifecycle hook.

Provides a hook that writes status events (running/complete/failed) to the event
store. Supports per-state messages, optional identity, and arbitrary detail payloads.
"""

from typing import Any, Optional
from pydantic import BaseModel, ConfigDict, Field, model_validator

import core_logging as log

from core_framework.models import DeploymentDetails, HookResource, HookResourceParameters
from core_db.event import EventActions, EventItem

from .hook import ActionHook


class StatusHookStateParamters(BaseModel):
    """Per-state parameters for the status hook.

    Attributes:
        status: One or more status values to set (e.g., ["OK"], ["ERROR"]).
        message: Optional human-readable message to record with the event.
    """

    model_config = ConfigDict(populate_by_name=True)

    status: list[str] = Field(..., description="The status to set", alias="Status")
    message: Optional[str] = Field(None, description="The message to set", alias="Message")

    def model_dump(self, **kwargs: Any) -> dict[str, Any]:
        """Return a dict suitable for serialization.

        Excludes unset and None fields by default.

        Args:
            **kwargs: Optional Pydantic dump options.

        Returns:
            A dict with aliases applied and None/unset values removed.
        """
        kwargs.setdefault("exclude_unset", True)
        kwargs.setdefault("by_alias", True)
        kwargs.setdefault("exclude_none", True)
        return super().model_dump(**kwargs)


class StatusHookParameters(HookResourceParameters):
    """Top-level parameters for the status hook.

    Inherits per-state fields (OnRunning/OnComplete/OnFailed) from BasehookParameters
    and adds optional identity and details fields that apply to all states.

    Attributes:
        identity: Optional identity/subject for the event (overrides default).
        details: Optional dict of additional details to attach to the event.
    """

    identity: Optional[str] = Field(None, description="The identity for the event", alias="Identity")
    details: Optional[dict] = Field(None, description="Additional details for the event", alias="Details")

    @model_validator(mode="before")
    @classmethod
    def validate_state_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Normalize per-state parameter payloads.

        Converts raw dicts under OnRunning/OnComplete/OnFailed into
        StatusHookStateParamters instances for consistent typing.

        Args:
            values: Raw parameters dict.

        Returns:
            Normalized parameters dict.
        """
        if not isinstance(values, dict):
            return values

        for state_key in ["on_running", "OnRunning", "on_complete", "OnComplete", "on_failed", "OnFailed"]:
            state_params = values.get(state_key)
            if isinstance(state_params, dict):
                values[state_key] = StatusHookStateParamters.model_validate(state_params)
            elif isinstance(state_params, StatusHookStateParamters):
                values[state_key] = state_params

        return values


class StatusHook(ActionHook):
    """Hook that emits status events for action lifecycle states.

    For each supported state ("running", "complete", "failed"), this hook can
    publish a Status event containing a status list and optional message/details.
    """

    def __init__(
        self,
        definition: HookResource,
        deployment_details: DeploymentDetails,
        context: dict[str, Any],
        parent: str,
    ) -> None:
        """Initialize the StatusHook.

        Args:
            definition: Hook resource definition (type, states, parameters).
            deployment_details: Deployment context used to populate PRN/scope.
            context: Variables available for rendering/payload enrichment.
            parent: Parent action name or identifier.
        """
        super().__init__(definition, deployment_details, context, parent)

        # Validate that parameters are provided
        self.parameters = StatusHookParameters(**definition.parameters)

    def execute(self, **kwargs) -> bool:
        """Emit a status event if the requested state is enabled.

        Args:
            **kwargs: Expected to include:
                - state: Lifecycle state ("running" | "complete" | "failed").
                - Any additional fields to include in the event details.

        Behavior:
            - If state is not configured on this hook, returns without action.
            - Builds an EventItem using deployment scope/PRN and per-state params.
            - Persists the event via EventActions.create().
        """
        try:
            state = str(kwargs.get("state", kwargs.get("State", ""))).lower()
            if not state in self.states:
                return

            client = self.deployment_details.client

            scope = self.deployment_details.get_scope()
            prn = self.deployment_details.get_prn()

            identity = self.parameters.identity or prn

            state_params = StatusHookStateParamters(**self.parameters.get_parameters(state))

            # Placeholder for actual execution logic
            event = EventItem(
                prn=identity,
                event_type="STATUS",
                item_type=scope,
                status=state_params.status,
                message=state_params.message,
                details=self.parameters.details,
            )

            EventActions.create(client=client, **event.model_dump())

            return True

        except Exception as e:
            # Log and suppress exceptions to avoid impacting main action flow
            log.error("StatusHook execution failed: {}", e, exc_info=True)
            return False
