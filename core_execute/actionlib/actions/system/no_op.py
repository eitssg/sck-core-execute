"""Perform a NoOps (No Operation) action"""

from typing import Any

import core_logging as log

from core_framework.models import DeploymentDetails, ActionDefinition, ActionParams

from core_execute.actionlib.action import BaseAction


def generate_template() -> ActionDefinition:
    """Generate the action definition"""

    definition = ActionDefinition(
        Label="action-definition-label",
        Type="SYSTEM::NoOp",
        DependsOn=["put-a-label-here"],
        Params=ActionParams(),
        Scope="",
    )

    return definition


class NoOpAction(BaseAction):
    """Perform a NoOps (No Operation) action

    There is no operation to perform

    Attributes:
        Type: Use the value: ``SYSTEM::NoOp``

    .. rubric: ActionDefinition:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Label: action-system-noop-label
              Type: "SYSTEM::NoOp"
              Params: {}
              Scope: "build"

    """

    def __init__(
        self,
        definition: ActionDefinition,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

    def _execute(self):

        log.trace("NoOpAction._execute()")

        self.set_complete("No operation required")

        log.trace("NoOpAction._execute() - complete")

    def _check(self):

        log.trace("NoOpAction._check()")

        self.set_complete("No operation required")

        log.trace("NoOpAction._check() - complete")

    def _unexecute(self):

        log.trace("NoOpAction._unexecute()")

        pass

    def _cancel(self):

        log.trace("NoOpAction._cancel()")

        pass

    def _resolve(self):

        log.trace("NoOpAction._resolve()")

        pass

    def _cleanup(self):

        log.trace("NoOpAction._cleanup()")

        pass
