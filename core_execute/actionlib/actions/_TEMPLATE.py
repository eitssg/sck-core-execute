from typing import Any

from core_framework.models import ActionDefinition, DeploymentDetails, ActionParams

from core_execute.actionlib.action import BaseAction

from core_renderer import Jinja2Renderer


def generate_template() -> ActionDefinition:
    """Generate the action definition"""

    definition = ActionDefinition(
        Label="action-definition-label",
        Type="SYSTEM::ActionNameGoesHere",
        DependsOn=['put-a-label-here'],
        Params=ActionParams(Account="See class ActionParams for details"),
        Scope="Based on your deployment details, it one of 'portfolio', 'app', 'branch', or 'build'",
    )

    return definition


class ActionNameGoesHereAction(BaseAction):

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
