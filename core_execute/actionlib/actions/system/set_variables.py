"""A method to set variables internally in memory and pass them through Jinja2 context rendering first"""

from typing import Any
from pydantic import Field, model_validator

import core_logging as log

from core_framework.models import DeploymentDetails, ActionResource, ActionSpec

from core_execute.actionlib.action import BaseAction


class SetVariablesActionSpec(ActionSpec):
    """Parameters for the SetVariablesAction"""

    variables: dict[str, Any] = Field(..., alias="Variables", description="The variables to set (required)")

    @model_validator(mode="before")
    @classmethod
    def validate_model_before(cls, values: Any) -> dict[str, Any]:
        if isinstance(values, dict):
            if not any(key in values for key in ["account", "Account"]):
                values["Account"] = "not-required"
            if not any(key in values for key in ["region", "Region"]):
                values["Region"] = "not-required"

        return values


class SetVariablesActionResource(ActionResource):

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:

        if not isinstance(values, dict):
            return values

        values.pop("kind", None)
        values.pop("Kind", None)
        values["kind"] = "SYSTEM::SetVariables"

        spec = values.pop("spec", None) or values.pop("Spec", None)
        if isinstance(spec, dict):
            values["spec"] = spec
        elif isinstance(spec, SetVariablesActionSpec):
            values["spec"] = spec.model_dump()

        return values


class SetVariablesAction(BaseAction[SetVariablesActionSpec]):
    """Set variables in memory and in your state

    This action will set variables in memory and in your state.  The action will return the variables set.

    Attributes:
        Kind: Use the value: ``SYSTEM::SetVariables``
        Spec.Variables: The variables to set (required)

    .. rubric: ActionResource:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Name: action-system-setvariables-name
                Kind: "SYSTEM::SetVariables"
                Spec:
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
        definition: ActionResource,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        self.spec = SetVariablesActionSpec.model_validate(definition.spec)

    def _execute(self):

        log.trace("SetVariablesAction._execute()")

        for key, value in self.spec.variables.items():
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

        for key in self.spec.variables:
            value = self.spec.variables[key]
            if isinstance(value, str):
                # Render the string using Jinja2 context
                result = self.renderer.render_string(value, self.context)

                # If the result can be converted to a number, do so, but if it's quoted, keep it as a string
                try:
                    result = float(result) if "." in result else int(result)
                except ValueError:
                    pass

                self.spec.variables[key] = result

        log.trace("SetVariablesAction._resolve()")

    @classmethod
    def generate_action_resource(cls, **kwargs) -> SetVariablesActionResource:
        return SetVariablesActionResource.model_validate(kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> SetVariablesActionSpec:
        return SetVariablesActionSpec.model_validate(kwargs)
