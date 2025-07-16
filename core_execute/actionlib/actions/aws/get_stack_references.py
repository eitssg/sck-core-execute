"""Gets the references to a stack output export"""

from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator

from botocore.exceptions import ClientError

import core_logging as log

from core_framework.models import DeploymentDetails, ActionSpec

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


class GetStackReferencesActionParams(BaseModel):
    """Parameters for the GetStackReferencesAction"""

    model_config = ConfigDict(populate_by_name=True, validate_assignment=True)

    account: str = Field(
        ..., alias="Account", description="The account to use for the action (required)"
    )
    region: str = Field(
        ..., alias="Region", description="The region to create the stack in (required)"
    )
    stack_name: str = Field(
        ...,
        alias="StackName",
        description="The name of the stack to check for references (required)",
    )
    output_name: str = Field(
        default="DefaultExport",
        alias="OutputName",
        description="The name of the output to check for references (optional) defaults to 'DefaultExport'",
    )


class GetStackReferencesActionSpec(ActionSpec):
    """Generate the action definition"""

    @model_validator(mode="before")
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Validate the parameters for the GetStackReferencesActionSpec"""
        if not (values.get("name") or values.get("Name")):
            values["name"] = "action-aws-getstackreferences-name"
        if not (values.get("kind") or values.get("Kind")):
            values["kind"] = "AWS::GetStackReferences"
        if not (values.get("depends_on") or values.get("DependsOn")):
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Params")):
            values["params"] = {
                "account": "",
                "region": "",
                "stack_name": "",
                "output_name": "DefaultExport",
            }
        return values


class GetStackReferencesAction(BaseAction):
    """Get the references to a stack output export variables

    This action will get the references to a stack output export variable.  The action will return the references to the export.

    Attributes:
        Kind: Use the value: ``AWS::GetStackReferences``
        Params.Account: The account where the stack is located
        Params.Region: The region where the stack is located
        Params.StackName: The name of the stack to check for references (required)
        Params.OutputName: The name of the output to check for references (optional) defaults to 'DefaultExport'

    .. rubric: ActionSpec:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Name: action-aws-getstackreferences-name
              Kind: "AWS::GetStackReferences"
              Params:
                Account: "154798051514"
                StackName: "my-stack-name"
                Region: "ap-southeast-1"
                OutputName: "DefaultExport"
              Scope: "build"

    """

    def __init__(
        self,
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        # Validate the action parameters
        self.params = GetStackReferencesActionParams(**definition.params)

    def _execute(self):

        log.trace("GetStackReferencesAction._execute()")

        # Obtain a CloudFormation client
        cfn_client = aws.cfn_client(
            region=self.params.Region,
            role=util.get_provisioning_role_arn(self.params.account),
        )

        output_export_name = "{}:{}".format(self.params.stack_name, self.output_name)
        try:
            response = cfn_client.list_imports(ExportName=output_export_name)

            # No error thrown - stack is being referenced
            log.debug(
                "Stack is being referenced",
                details={
                    "StackName": self.params.stack_name,
                    "OutputName": self.output_name,
                    "References": response["Imports"],
                    "HasReferences": True,
                    "NumReferences": len(response["Imports"]),
                },
            )

            # Set outputs
            self.set_output("References", response["Imports"])
            self.set_output("HasReferences", True)
            self.set_output("NumReferences", len(response["Imports"]))

            # Complete the action
            self.set_complete("Stack '{}' is referenced".format(self.params.stack_name))

        except ClientError as e:
            # Error thrown - stack is not being referenced (or a legit error)
            self.set_output("References", [])
            self.set_output("HasReferences", False)
            self.set_output("NumReferences", 0)
            if "does not exist" in e.response["Error"]["Message"]:
                # Export doesn't exist - treat as unreferenced stack
                self.set_complete(
                    "Output export '{}' does not exist, treating stack as unreferenced".format(
                        output_export_name
                    )
                )
            elif "not imported" in e.response["Error"]["Message"]:
                # Export isn't imported / referenced
                log.warning(
                    "Stack '{}' is not referenced",
                    details={
                        "StackName": self.params.stack_name,
                        "OutputName": self.output_name,
                    },
                )
                self.set_complete(
                    "Stack '{}' is not referenced".format(self.params.stack_name)
                )
            else:
                # Other error
                log.error(
                    "Error getting references for stack '{}': {}",
                    self.params.stack_name,
                    e,
                )
                raise

        log.trace("GetStackReferencesAction._execute() complete")

    def _check(self):

        log.trace("GetStackReferencesAction._check()")

        self.set_failed("Internal error - _check() should not have been called")

        log.trace("GetStackReferencesAction._check() complete")

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):

        log.trace("GetStackReferencesAction._resolve()")

        self.params.account = self.renderer.render_string(
            self.params.account, self.context
        )
        self.params.region = self.renderer.render_string(
            self.params.region, self.context
        )
        self.params.stack_name = self.renderer.render_string(
            self.params.stack_name, self.context
        )
        self.output_name = self.renderer.render_string(self.output_name, self.context)

        log.trace("GetStackReferencesAction._resolve() complete")
