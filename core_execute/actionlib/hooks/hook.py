"""Hook base classes and models.

Defines:
- VALID_STATES: Allowed lifecycle states a hook can target.
- BasehookParameters: Per-state parameter container (running/complete/failed).
- HookResource: Declarative hook definition (type, states, parameters).
- ActionHook: Runtime base class that concrete hooks extend.

These classes are used to configure and execute lifecycle hooks around actions.
"""

from typing import Any
from core_framework.models import DeploymentDetails, HookResource


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
