from typing import Any

import core_logging as log
from botocore.exceptions import ClientError

from core_framework.models import DeploymentDetails, ActionDefinition

import core_helper.aws as aws

import core_execute.envinfo as envinfo
from core_execute.actionlib.action import BaseAction


class DeleteStackAction(BaseAction):

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
        self.success_statuses = self.params.SuccessStatuses or []

    def _execute(self):
        # Obtain a CloudFormation client
        cfn_client = aws.cfn_client(
            region=self.region, role=envinfo.provisioning_role_arn(self.account)
        )

        # Describe the stack to get its status
        exists = True
        try:
            cfn_response = cfn_client.describe_stacks(StackName=self.stack_name)
            stack_status = cfn_response["Stacks"][0]["StackStatus"]
            stack_id = cfn_response["Stacks"][0]["StackId"]
            self.set_state("StackId", stack_id)
            self.set_output("StackStatus", stack_status)
        except ClientError as e:
            if "does not exist" in e.response["Error"]["Message"]:
                exists = False
            else:
                raise

        if exists is False:
            self.set_output("StackStatus", "DELETE_COMPLETE")
            self.set_complete(
                "Stack '{}' does not exist, it may have been previously deleted".format(
                    self.stack_name
                )
            )
        elif stack_status == "DELETE_COMPLETE":
            self.set_output("StackStatus", stack_status)
            self.set_complete("Stack '{}' has been previously deleted")
        else:
            self.set_running("Deleting stack '{}'".format(self.stack_name))
            cfn_response = cfn_client.delete_stack(StackName=self.get_state("StackId"))

    def _check(self):
        # Obtain a CloudFormation client
        cfn_client = aws.cfn_client(
            region=self.region, role=envinfo.provisioning_role_arn(self.account)
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
                exists = False
            else:
                raise

        if exists is False or stack_status == "DELETE_COMPLETE":
            self.set_output("StackStatus", "DELETE_COMPLETE")
            self.set_complete()
        elif stack_status in self.success_statuses:
            self.set_output("StackStatus", stack_status)
            self.set_complete(
                "Stack was not deleted because it is still in use, configured as success"
            )
        elif "IN_PROGRESS" in stack_status:
            log.debug("Stack status is '{}'", stack_status)
        else:
            self.set_output("StackStatus", stack_status)
            self.set_failed("Stack status is '{}'".format(stack_status))

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):
        self.account = self.renderer.render_string(self.account, self.context)
        self.region = self.renderer.render_string(self.region, self.context)
        self.stack_name = self.renderer.render_string(self.stack_name, self.context)
