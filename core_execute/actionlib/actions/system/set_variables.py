"""A method to set variables internally in memory and pass them through Jinja2 context rendering first"""
from typing import Any
from core_framework.models import DeploymentDetails, ActionDefinition, ActionParams

from ...action import BaseAction


def generate_template() -> ActionDefinition:
    """Generate the action definition"""

    definition = ActionDefinition(
        Label="action-definition-label",
        Type="SYSTEM::SetVariables",
        DependsOn=['put-a-label-here'],
        Params=ActionParams(
            Variables={"any": "The variables to set (required)"},
        ),
        Scope="",
    )

    return definition


class SetVariablesAction(BaseAction):

    variables: dict[str, str] | None = None

    def __init__(
        self,
        definition: ActionDefinition,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        self.variables = self.params.Variables

    def _execute(self):
        for key, value in self.variables.items():
            self.set_output(key, value)
            self.set_state(key, value)

        self.set_complete()

    def _check(self):
        self.set_failed("Internal error - _check() should not have been called")

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):
        for key in self.variables:
            self.variables[key] = self.renderer.render_string(
                self.variables[key], self.context
            )
