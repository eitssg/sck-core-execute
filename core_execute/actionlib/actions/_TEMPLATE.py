from typing import Any

from core_framework.models import ActionDefinition, DeploymentDetails, ActionParams

from core_execute.actionlib.action import BaseAction

from core_renderer import Jinja2Renderer


def generate_template() -> ActionDefinition:
    """Generate the action definition"""

    definition = ActionDefinition(
        Label="action-definition-label",
        Type="SYSTEM::ActionNameGoesHere",
        DependsOn=["put-a-label-here"],
        Params=ActionParams(Account="See class ActionParams for details"),
        Scope="Based on your deployment details, it one of 'portfolio', 'app', 'branch', or 'build'",
    )

    return definition


class ActionNameGoesHereAction(BaseAction):
    """Sameple Action Description

    Attributes:
        Type: Use the value: ``SYSTEM::ActionNameGoesHere``
        Params.Account: The account where the action is located
        Params.Region: The region where the action is located
        Params.ActionName: The name of the action to execute

    .. rubric: ActionDefinition:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Label: action-system-actionnamegoeshere-label
              Type: "SYSTEM::ActionNameGoesHere"
              Params:
                Account: "154798051514"
                Region: "ap-southeast-1"
                ActionName: "my-action-name"
              Scope: "build"
    """

    renderer = Jinja2Renderer()

    def __init__(
        self,
        definition: ActionDefinition,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)
        # TODO: process parameters from definition

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
