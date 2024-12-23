"""Perform a NoOps (No Operation) action"""

from typing import Any

from core_framework.models import DeploymentDetails, ActionDefinition, ActionParams
from ...action import BaseAction


def generate_template() -> ActionDefinition:
    """Generate the action definition"""

    definition = ActionDefinition(
        Label="action-definition-label",
        Type="SYSTEM::NoOp",
        DependsOn=['put-a-label-here'],
        Params=ActionParams(),
        Scope="",
    )

    return definition


class NoOpAction(BaseAction):
    def __init__(
        self,
        definition: ActionDefinition,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

    def _execute(self):
        self.set_complete("No operation required")

    def _check(self):
        self.set_complete("No operation required")

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):
        pass
