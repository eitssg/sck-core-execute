from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator
from core_framework.models import ActionResource, DeploymentDetails, ActionSpec

from core_execute.actionlib.action import BaseAction

from core_renderer import Jinja2Renderer


class ActionNameGoesHereActionSpec(ActionSpec):
    """Parameters for the ActionNameGoesHereAction

    This class defines the parameters that can be used in the action.
    You can add more attributes as needed.
    """

    pass


class ActionNameGoesHereActionResource(ActionResource):
    """Generate the action definition"""

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:

        if not isinstance(values, dict):
            return values

        values.pop("kind", None)
        values.pop("Kind", None)
        values["kind"] = "TEMPLATE::ActionNameGoesHere"

        spec = values.pop("spec", None) or values.pop("Spec", None)
        if isinstance(spec, dict):
            values["spec"] = spec
        elif isinstance(spec, ActionNameGoesHereActionSpec):
            values["spec"] = spec.model_dump()

        return values


class ActionNameGoesHereAction(BaseAction[ActionNameGoesHereActionSpec]):
    """Sameple Action Description

    Kind: Use the value: ``SYSTEM::ActionNameGoesHere``

    .. rubric: ActionResource:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Name: action-system-actionnamegoeshere-name
              Kind: "SYSTEM::ActionNameGoesHere"
              Spec:
                Account: "154798051514"
                Region: "ap-southeast-1"
              Scope: "build"
    """

    renderer = Jinja2Renderer()

    def __init__(
        self,
        definition: ActionResource,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        self.spec = ActionNameGoesHereActionSpec(**definition.spec)

    def _execute(self):
        # TODO: implement action execution
        pass

    def _check(self):
        # TODO: implement action execution status check
        pass

    def _unexecute(self):
        # TODO: implement action reverse execution
        pass

    def _cancel(self):
        # TODO: implement action execution cancellation
        pass

    def _resolve(self):
        # TODO: implement runtime resolution of action variables
        pass

    @classmethod
    def generate_action_resource(cls, **kwargs) -> ActionNameGoesHereActionResource:
        return ActionNameGoesHereActionResource(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> ActionNameGoesHereActionSpec:
        return ActionNameGoesHereActionSpec(**kwargs)
