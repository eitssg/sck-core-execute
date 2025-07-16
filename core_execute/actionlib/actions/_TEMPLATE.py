from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator
from core_framework.models import ActionSpec, DeploymentDetails

from core_execute.actionlib.action import BaseAction

from core_renderer import Jinja2Renderer


class TemplateActionParams(BaseModel):
    """Parameters for the ActionNameGoesHereAction

    This class defines the parameters that can be used in the action.
    You can add more attributes as needed.
    """

    model_config = ConfigDict(populate_by_name=True, validate_assignment=True)

    account: str = Field(
        ..., alias="Account", description="The account where the action is located"
    )
    region: str = Field(
        ..., alias="Region", description="The region where the action is located"
    )


class TemplateActionSpec(ActionSpec):
    """Generate the action definition"""

    @model_validator(mode="before")
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Validate the parameters for the TemplateActionSpec"""
        if not (values.get("name") or values.get("Name")):
            values["name"] = "action-system-actionnamegoeshere-name"
        if not (values.get("kind") or values.get("Kind")):
            values["kind"] = "SYSTEM::ActionNameGoesHere"
        if not (values.get("depends_on") or values.get("DependsOn")):
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Params")):
            values["params"] = {
                "account": "",
                "region": "",
            }
        return values


class ActionNameGoesHereAction(BaseAction):
    """Sameple Action Description

    Kind: Use the value: ``SYSTEM::ActionNameGoesHere``

    .. rubric: ActionSpec:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Name: action-system-actionnamegoeshere-name
              Kind: "SYSTEM::ActionNameGoesHere"
              Params:
                Account: "154798051514"
                Region: "ap-southeast-1"
              Scope: "build"
    """

    renderer = Jinja2Renderer()

    def __init__(
        self,
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        self.params = TemplateActionParams(**definition.params)

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
