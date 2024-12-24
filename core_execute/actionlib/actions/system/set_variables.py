"""A method to set variables internally in memory and pass them through Jinja2 context rendering first"""

from typing import Any

import core_logging as log

from core_framework.models import DeploymentDetails, ActionDefinition, ActionParams

from core_execute.actionlib.action import BaseAction


def generate_template() -> ActionDefinition:
    """Generate the action definition"""

    definition = ActionDefinition(
        Label="action-definition-label",
        Type="SYSTEM::SetVariables",
        DependsOn=["put-a-label-here"],
        Params=ActionParams(
            Variables={"any": "The variables to set (required)"},
        ),
        Scope="",
    )

    return definition


class SetVariablesAction(BaseAction):
    """Set variables in memory and in your state

    This action will set variables in memory and in your state.  The action will return the variables set.

    Attributes:
        Type: Use the value: ``SYSTEM::SetVariables``
        Params.Variables: The variables to set (required)

    .. rubric: ActionDefinition:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Label: action-system-setvariables-label
                Type: "SYSTEM::SetVariables"
                Params:
                    Variables:
                        Name: "John Smith"
                        Age: "25"
                        Height: "6'2"
                        Weight: "180"
                Scope: "build"

    """

    variables: dict[str, str] | None = None

    def __init__(
        self,
        definition: ActionDefinition,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

    def _execute(self):

        log.trace("SetVariablesAction._execute()")

        for key, value in self.params.Variables.items():
            self.set_output(key, value)
            self.set_state(key, value)

        self.set_complete()

        log.trace("SetVariablesAction._execute() - complete")

    def _check(self):

        log.trace("SetVariablesAction._check()")

        self.set_failed("Internal error - _check() should not have been called")

        log.trace("SetVariablesAction._check()")

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):

        log.trace("SetVariablesAction._resolve()")

        for key in self.params.Variables:
            self.params.Variables[key] = self.renderer.render_string(
                self.params.Variables[key], self.context
            )

        log.trace("SetVariablesAction._resolve()")
