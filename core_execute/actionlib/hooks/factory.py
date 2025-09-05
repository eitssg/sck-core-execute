from core_framework.models import DeploymentDetails
from .hook import ActionHook, HookResource
from .status import StatusHook


LC_TYPE_STATUS = "status"


class HookFactory(object):
    """Factory class to create hook instances based on their type."""

    @staticmethod
    def load(
        hook: HookResource,
        deployment_details: DeploymentDetails,
        context: dict,
        parent: str,
    ) -> ActionHook:
        """Factory function to create a hook instance based on its type.

        Args:
            lifecycle_hook: The lifecycle hook definition as a dictionary.

        Returns:
            An instance of a subclass of BaseHook corresponding to the hook type.

        Raises:
            ValueError: If the hook type is unknown or if the hook definition is invalid.
        """

        # We have created only one lifecycle hook so far, but this is where we would
        # extend to support more types in the future.
        HOOKS_REGISTROY = {
            LC_TYPE_STATUS: StatusHook,
        }

        hook_class = HOOKS_REGISTROY.get(hook.type.lower())
        if not hook_class:
            raise ValueError(f"Unknown hook type: {hook.type}")

        return hook_class(hook, deployment_details, context, parent)
