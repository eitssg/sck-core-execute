"""Perform a NoOps (No Operation) action"""

from typing import Any
from pydantic import BaseModel, ConfigDict, model_validator
from datetime import datetime, timezone

import core_logging as log

from core_framework.models import DeploymentDetails, ActionSpec, ActionParams

from core_execute.actionlib.action import BaseAction


class NoOpActionParams(ActionParams):
    """Parameters for the NoOpAction"""

    @model_validator(mode="before")
    @classmethod
    def validate_model_before(cls, values: Any) -> dict[str, Any]:
        if isinstance(values, dict):
            if not any(key in values for key in ["account", "Account"]):
                values["Account"] = "not-required"
            if not any(key in values for key in ["region", "Region"]):
                values["Region"] = "not-required"

        return values


class NoOpActionSpec(ActionSpec):
    """Generate the action specification for the NoOp action"""

    @model_validator(mode="before")
    def validate_params(cls, values) -> dict:

        if not (values.get("name") or values.get("Name")):
            values["name"] = "action-system-noop-name"

        if not (values.get("kind") or values.get("Kind")):
            values["kind"] = "SYSTEM::NoOp"

        if not (values.get("depends_on") or values.get("DependsOn")):
            values["depends_on"] = []

        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"

        if not (values.get("params") or values.get("Params")):
            values["params"] = {}

        return values


class NoOpAction(BaseAction):
    """Perform a NoOps (No Operation) action

    There is no operation to perform

    Attributes:
        Kind: Use the value: ``SYSTEM::NoOp``

    .. rubric: ActionSpec:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Name: action-system-noop-name
              Kind: "SYSTEM::NoOp"
              Params: {}
              Scope: "build"

    """

    def __init__(
        self,
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        self.params = NoOpActionParams(**definition.params)

    @classmethod
    def generate_spec(cls, action_spec: dict) -> ActionSpec:
        """
        Return the action specification for the NoOp action
        This will be a generic class method called from the helper or action factory.
        """
        return NoOpActionSpec(**action_spec)

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

    @classmethod
    def generate_action_spec(cls, **kwargs) -> NoOpActionSpec:
        return NoOpActionSpec(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> NoOpActionParams:
        return NoOpActionParams(**kwargs)
