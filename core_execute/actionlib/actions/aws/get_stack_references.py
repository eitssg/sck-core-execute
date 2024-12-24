"""Gets the references to a stack output export"""

from typing import Any
from botocore.exceptions import ClientError

import core_logging as log

from core_framework.models import DeploymentDetails, ActionDefinition, ActionParams

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


def generate_template() -> ActionDefinition:
    """Generate the action definition"""

    definition = ActionDefinition(
        Label="action-definition-label",
        Type="AWS::GetStackReferences",
        DependsOn=["put-a-label-here"],
        Params=ActionParams(
            Account="The account to use for the action (required)",
            Region="The region to create the stack in (required)",
            StackName="The name of the stack to check for references (required)",
            OutputName="The name of the output to check for references (optional) defaults to 'DefaultExport'",
        ),
        Scope="Based on your deployment details, it one of 'portfolio', 'app', 'branch', or 'build'",
    )

    return definition


class GetStackReferencesAction(BaseAction):
    """Get the references to a stack output export variables

    This action will get the references to a stack output export variable.  The action will return the references to the export.

    Attributes:
        Type: Use the value: ``AWS::GetStackReferences``
        Params.Account: The account where the stack is located
        Params.Region: The region where the stack is located
        Params.StackName: The name of the stack to check for references (required)
        Params.OutputName: The name of the output to check for references (optional) defaults to 'DefaultExport'

    .. rubric: ActionDefinition:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Label: action-aws-getstackreferences-label
              Type: "AWS::GetStackReferences"
              Params:
                Account: "154798051514"
                StackName: "my-stack-name"
                Region: "ap-southeast-1"
                OutputName: "DefaultExport"
              Scope: "build"

    """

    def __init__(
        self,
        definition: ActionDefinition,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        if self.params.OutputName is None:
            self.output_name = "DefaultExport"

    def _execute(self):

        log.trace("GetStackReferencesAction._execute()")

        # Obtain a CloudFormation client
        cfn_client = aws.cfn_client(
            region=self.params.Region,
            role=util.get_provisioning_role_arn(self.params.Account),
        )

        output_export_name = "{}:{}".format(self.params.StackName, self.output_name)
        try:
            response = cfn_client.list_imports(ExportName=output_export_name)

            # No error thrown - stack is being referenced
            log.debug(
                "Stack is being referenced",
                details={
                    "StackName": self.params.StackName,
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
            self.set_complete("Stack '{}' is referenced".format(self.params.StackName))

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
                        "StackName": self.params.StackName,
                        "OutputName": self.output_name,
                    },
                )
                self.set_complete(
                    "Stack '{}' is not referenced".format(self.params.StackName)
                )
            else:
                # Other error
                log.error(
                    "Error getting references for stack '{}': {}",
                    self.params.StackName,
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

        self.params.Account = self.renderer.render_string(
            self.params.Account, self.context
        )
        self.params.Region = self.renderer.render_string(
            self.params.Region, self.context
        )
        self.params.StackName = self.renderer.render_string(
            self.params.StackName, self.context
        )
        self.output_name = self.renderer.render_string(self.output_name, self.context)

        log.trace("GetStackReferencesAction._resolve() complete")
