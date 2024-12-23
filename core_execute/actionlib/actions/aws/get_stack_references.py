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
        DependsOn=['put-a-label-here'],
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
    def __init__(
        self,
        definition: ActionDefinition,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        self.account = self.params.Account
        self.region = self.params.Region
        self.stack_name = self.params.StackName
        self.output_name = self.params.OutputName or "DefaultExport"

    def _execute(self):
        # Obtain a CloudFormation client
        cfn_client = aws.cfn_client(
            region=self.region, role=util.get_provisioning_role_arn(self.account)
        )

        output_export_name = "{}:{}".format(self.stack_name, self.output_name)
        try:
            response = cfn_client.list_imports(ExportName=output_export_name)

            # No error thrown - stack is being referenced
            log.debug(
                "Stack is being referenced",
                details={
                    "StackName": self.stack_name,
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
            self.set_complete("Stack '{}' is referenced".format(self.stack_name))

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
                self.set_complete(
                    "Stack '{}' is not referenced".format(self.stack_name)
                )
            else:
                # Other error
                raise

    def _check(self):
        self.set_failed("Internal error - _check() should not have been called")

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):
        self.account = self.renderer.render_string(self.account, self.context)
        self.region = self.renderer.render_string(self.region, self.context)
        self.stack_name = self.renderer.render_string(self.stack_name, self.context)
        self.output_name = self.renderer.render_string(self.output_name, self.context)
