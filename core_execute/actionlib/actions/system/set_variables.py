"""A method to set variables internally in memory and pass them through Jinja2 context rendering first"""

from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator

import core_logging as log

from core_framework.models import DeploymentDetails, ActionSpec

from core_execute.actionlib.action import BaseAction


class SetVariablesActionParams(BaseModel):
    """Parameters for the SetVariablesAction"""

    model_config = ConfigDict(populate_by_name=True, validate_assignment=True)

    variables: dict[str, Any] = Field(
        ..., alias="Variables", description="The variables to set (required)"
    )


class SetVariablesActionSpec(ActionSpec):

    @model_validator(mode="before")
    def validate_params(cls, values: dict) -> dict:

        if not (values.get("label") or values.get("Label")):
            values["label"] = "action-system-set-variables-label"
        if not (values.get("type") or values.get("Type")):
            values["type"] = "SYSTEM::SetVariables"
        if not (values.get("depends_on") or values.get("DependsOn")):
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Params")):
            values["params"] = {"variables": {}}
        return values


class SetVariablesAction(BaseAction):
    """Set variables in memory and in your state

    This action will set variables in memory and in your state.  The action will return the variables set.

    Attributes:
        Type: Use the value: ``SYSTEM::SetVariables``
        Params.Variables: The variables to set (required)

    .. rubric: ActionSpec:

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
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        self.params = SetVariablesActionParams(**definition.params)

    def _execute(self):

        log.trace("SetVariablesAction._execute()")

        for key, value in self.params.variables.items():
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

        for key in self.params.variables:
            self.params.variables[key] = self.renderer.render_string(
                self.params.variables[key], self.context
            )

        log.trace("SetVariablesAction._resolve()")
