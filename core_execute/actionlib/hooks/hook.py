"""Hook base classes and models.

Defines:
- VALID_STATES: Allowed lifecycle states a hook can target.
- BasehookParameters: Per-state parameter container (running/complete/failed).
- HookResource: Declarative hook definition (type, states, parameters).
- ActionHook: Runtime base class that concrete hooks extend.

These classes are used to configure and execute lifecycle hooks around actions.
"""

from typing import Any, Optional
from pydantic import BaseModel, ConfigDict, Field, model_validator

from core_framework.models import DeploymentDetails

VALID_STATES = ["running", "complete", "failed"]


class BasehookParameters(BaseModel):
    """Container for per-state hook parameters.

    Attributes:
        on_running: Parameters to use when state == "running".
        on_complete: Parameters to use when state == "complete".
        on_failed: Parameters to use when state == "failed".
    """

    model_config = ConfigDict(populate_by_name=True)

    on_running: Optional[dict] = Field(None, description="Message when running", alias="OnRunning")
    on_complete: Optional[dict] = Field(None, description="Message when complete", alias="OnComplete")
    on_failed: Optional[dict] = Field(None, description="Message when failed", alias="OnFailed")

    def get_parameters(self, state: str) -> dict[str, Any]:
        """Return parameters for the given state.

        Args:
            state: One of "running", "complete", or "failed".

        Returns:
            A dict of parameters for the state. Returns an empty dict if not defined.
        """
        if state == "running":
            return self.on_running or {}
        elif state == "complete":
            return self.on_complete or {}
        elif state == "failed":
            return self.on_failed or {}
        return {}

    def set_parameters(self, state: str, **kwargs) -> None:
        """Set parameters for the given state.

        Args:
            state: One of "running", "complete", or "failed".
            **kwargs: Key/value parameters to set for the state.
        """
        if state == "running":
            self.on_running = kwargs
        elif state == "complete":
            self.on_complete = kwargs
        elif state == "failed":
            self.on_failed = kwargs

    def model_dump(self, **kwargs: Any) -> dict[str, Any]:
        """Return a dict representation excluding unset/None fields.

        Args:
            **kwargs: Optional pydantic dump options.

        Returns:
            A dict suitable for serialization.
        """
        kwargs.setdefault("exclude_unset", True)
        kwargs.setdefault("by_alias", True)
        kwargs.setdefault("exclude_none", True)
        return super().model_dump(**kwargs)


class HookResource(BaseModel):
    """Declarative hook resource.

    Attributes:
        type: Hook type identifier (e.g., "Slack", "Webhook").
        states: List of states this hook should handle ("running", "complete", "failed").
        parameters: BasehookParameters containing per-state config.
    """

    model_config = ConfigDict(populate_by_name=True)

    type: str = Field(..., description="The type of hook", alias="Type")
    states: list[str] = Field(..., description="The states to accept", alias="States")
    parameters: BasehookParameters = Field(..., description="The parameters for the hook", alias="Parameters")

    @model_validator(mode="before")
    @classmethod
    def validate_parameters(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Normalize incoming values (states) before model creation.

        - Accepts States as a comma-delimited string or list.
        - Normalizes to lowercase.
        - Filters out values not in VALID_STATES.
        """
        if not isinstance(values, dict):
            return values

        states = values.pop("states", None) or values.pop("States", None)
        if isinstance(states, str):
            states = states.split(",")

        values["states"] = [s.lower() for s in states if s and s.strip().lower() in VALID_STATES] if states else []

        return values

    def model_dump(self, **kwargs: Any) -> dict[str, Any]:
        """Return a dict representation excluding unset/None fields.

        Args:
            **kwargs: Optional pydantic dump options.

        Returns:
            A dict suitable for serialization.
        """
        kwargs.setdefault("exclude_unset", True)
        kwargs.setdefault("by_alias", True)
        kwargs.setdefault("exclude_none", True)
        return super().model_dump(**kwargs)

    def set_parameters(self, state: str, **kwargs: Any) -> None:
        """Set parameters for a state on this hook.

        Args:
            state: One of "running", "complete", or "failed".
            **kwargs: Key/value parameters to set for the state.
        """
        if not self.parameters:
            self.parameters = BasehookParameters()
        self.parameters.set_parameters(state, **kwargs)

    def get_parameters(self, state: str) -> Optional[dict[str, Any]]:
        """Get parameters for a given state from this hook.

        Args:
            state: One of "running", "complete", or "failed".

        Returns:
            The parameter dict for the state, or None if parameters are not set.
        """
        if not self.parameters:
            return None
        return self.parameters.get_parameters(state)

    def execute(self, *, state: str, **kwargs) -> None:
        """Execute the hook for the given state.

        To be implemented by subclasses.

        Args:
            state: The lifecycle state triggering the hook ("running", "complete", "failed").
            **kwargs: Additional parameters/context for execution.

        Raises:
            NotImplementedError: Always, unless overridden by a subclass.
        """
        raise NotImplementedError("Execute method must be implemented by subclasses.")


class ActionHook(object):
    """Runtime base class for concrete hooks.

    Subclasses should implement execute() to perform side effects (e.g., post to
    Slack, call a webhook) based on the provided state and parameters.
    """

    def __init__(
        self,
        definition: HookResource,
        deployment_details: DeploymentDetails,
        context: dict[str, Any],
        parent: str,
    ) -> None:
        """Initialize the hook instance.

        Args:
            definition: HookResource model describing type/states/parameters.
            deployment_details: Deployment context for templating/auditing.
            context: Rendering/context variables available to the hook.
            parent: Parent action name or identifier emitting this hook.
        """
        self.type = definition.type
        self.states = definition.states
        self.parameters = definition.parameters
        self.deployment_details = deployment_details
        self.context = context
        self.parent = parent

    def execute(self, **kwargs) -> None:
        """Execute the hook.

        Subclasses must override this method.

        Args:
            state: The lifecycle state triggering the hook ("running", "complete", "failed").
            **kwargs: Additional parameters/context for execution.

        Raises:
            NotImplementedError: Always, unless overridden by a subclass.
        """
        raise NotImplementedError("Execute method must be implemented by subclasses.")
