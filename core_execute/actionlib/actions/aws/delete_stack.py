"""Teardown or delete a CloudFormation stack"""

from typing import Any

import core_logging as log
from botocore.exceptions import ClientError

from core_framework.models import DeploymentDetails, ActionDefinition, ActionParams

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


def generate_template() -> ActionDefinition:
    """Generate the action definition"""

    definition = ActionDefinition(
        Label="action-definition-label",
        Type="AWS::DeleteStack",
        DependsOn=["put-a-label-here"],
        Params=ActionParams(
            Account="The account to use for the action (required)",
            Region="The region to create the stack in (required)",
            StackName="The name of the stack to delete (required)",
            SuccessStatuses=[
                "The stack statuses that indicate success (optional). Defaults to []"
            ],
        ),
        Scope="Based on your deployment details, it one of 'portfolio', 'app', 'branch', or 'build'",
    )

    return definition


class DeleteStackAction(BaseAction):
    """Delete a CloudFormation stack

    This action will delete a CloudFormation stack.  The action will wait for the deletion to complete before returning.

    This process is typiclly part of a "teardown" task.

    Attributes:
        Type: Use the value: ``AWS::DeleteStack``
        Params.Account: The account where the stack is located
        Params.Region: The region where the stack is located
        Params.StackName: The name of the stack to delete (required)
        Params.SuccessStatuses: The stack statuses that indicate success (optional).  Defaults to []

    .. rubric:: ActionDefinition:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Label: action-aws-deletestack-label
              Type: "AWS::DeleteStack"
              Params:
                Account: "154798051514"
                Region: "ap-southeast-1"
                StackName: "my-appication-stack-name"
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

        log.trace("DeleteStackAction._execute()")

        # Obtain a CloudFormation client
        cfn_client = aws.cfn_client(
            region=self.params.Region,
            role=util.get_provisioning_role_arn(self.params.Account),
        )

        # Describe the stack to get its status
        exists = True
        try:
            cfn_response = cfn_client.describe_stacks(StackName=self.params.StackName)
            stack_status = cfn_response["Stacks"][0]["StackStatus"]
            stack_id = cfn_response["Stacks"][0]["StackId"]
            self.set_state("StackId", stack_id)
            self.set_output("StackStatus", stack_status)
        except ClientError as e:
            if "does not exist" in e.response["Error"]["Message"]:
                log.warning("Stack '{}' does not exist", self.params.StackName)
                exists = False
            else:
                log.error("Error describing stack '{}': {}", self.params.StackName, e)
                raise

        if exists is False:
            self.set_output("StackStatus", "DELETE_COMPLETE")
            self.set_complete(
                "Stack '{}' does not exist, it may have been previously deleted".format(
                    self.params.StackName
                )
            )
        elif stack_status == "DELETE_COMPLETE":
            self.set_output("StackStatus", stack_status)
            self.set_complete("Stack '{}' has been previously deleted")
        else:
            self.set_running("Deleting stack '{}'".format(self.params.StackName))
            cfn_response = cfn_client.delete_stack(StackName=self.get_state("StackId"))

        log.trace("DeleteStackAction._execute() complete")

    def _check(self):

        log.trace("DeleteStackAction._check()")

        # Obtain a CloudFormation client
        cfn_client = aws.cfn_client(
            region=self.params.Region,
            role=util.get_provisioning_role_arn(self.params.Account),
        )

        # Describe the stack to get its status
        exists = True
        try:
            cfn_response = cfn_client.describe_stacks(
                StackName=self.get_state("StackId")
            )
            stack_status = cfn_response["Stacks"][0]["StackStatus"]
        except ClientError as e:
            if "does not exist" in e.response["Error"]["Message"]:
                log.warning("Stack '{}' does not exist", self.params.StackName)
                exists = False
            else:
                log.error("Error describing stack '{}': {}", self.params.StackName, e)
                raise

        if exists is False or stack_status == "DELETE_COMPLETE":
            self.set_output("StackStatus", "DELETE_COMPLETE")
            self.set_complete()
        elif (
            not self.params.SuccessStatuses
            or stack_status in self.params.SuccessStatuses
        ):
            self.set_output("StackStatus", stack_status)
            self.set_complete(
                "Stack was not deleted because it is still in use, configured as success"
            )
        elif "IN_PROGRESS" in stack_status:
            log.debug("Stack status is '{}'", stack_status)
        else:
            self.set_output("StackStatus", stack_status)
            self.set_failed("Stack status is '{}'".format(stack_status))

        log.trace("DeleteStackAction._check() complete")

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):

        log.trace("DeleteStackAction._resolve()")

        self.params.Account = self.renderer.render_string(
            self.params.Account, self.context
        )
        self.params.Region = self.renderer.render_string(
            self.params.Region, self.context
        )
        self.params.StackName = self.renderer.render_string(
            self.params.StackName, self.context
        )

        log.trace("DeleteStackAction._resolve()")
