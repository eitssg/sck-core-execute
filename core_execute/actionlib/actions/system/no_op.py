"""Perform a NoOps (No Operation) action"""

from typing import Any
from datetime import datetime, timezone

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

        log.debug("Execute NoOp Action. Setting NoOpAction as complete")

        t = datetime.now(timezone.utc).isoformat()

        log.debug("Setting output variable: execution_time=`{}`", t)

        self.set_output("execution_time", t)

        self.set_complete("NoOpAction Execution Complete!")

        log.trace("NoOpAction._execute() - complete")

    def _check(self):

        log.trace("NoOpAction._check()")

        log.debug("Checking NoOp Action. Setting NoOpAction as complete")

        self.set_complete("Check NoOp Action")

        log.trace("NoOpAction._check() - complete")

    def _unexecute(self):

        log.trace("NoOpAction._unexecute()")

        log.debug("Unexecuting NoOp Action")

        pass

    def _cancel(self):

        log.trace("NoOpAction._cancel()")

        log.debug("Cancelling NoOp Action")

        pass

    def _resolve(self):

        log.trace("NoOpAction._resolve()")

        log.debug("Resolving NoOp Action")

        pass

    def _cleanup(self):

        log.trace("NoOpAction._cleanup()")

        log.debug("Cleaning up NoOp Action")

        pass
