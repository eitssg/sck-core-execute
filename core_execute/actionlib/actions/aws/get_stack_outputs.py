from typing import Any

from botocore.exceptions import ClientError

import core_logging as log

from core_framework.models import DeploymentDetails, ActionDefinition

import core_helper.aws as aws

import core_execute.envinfo as envinfo
from core_execute.actionlib.action import BaseAction


class GetStackOutputsAction(BaseAction):
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

    def _execute(self):
        # Obtain a CloudFormation client
        cfn_client = aws.cfn_client(
            region=self.region, role=envinfo.provisioning_role_arn(self.account)
        )

        try:
            describe_stack_response = cfn_client.describe_stacks(
                StackName=self.stack_name
            )
            stack_id = describe_stack_response["Stacks"][0]["StackId"]
            self.__save_stack_outputs(describe_stack_response)
            self.set_state("StackId", stack_id)
            self.set_complete()
        except ClientError as e:
            if "does not exist" in e.response["Error"]["Message"]:
                log.debug(
                    "Stack '{}' does not exist, could not retrieve stack outputs",
                    self.stack_name,
                )
                self.set_complete()
            else:
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

    def __save_stack_outputs(self, describe_stack_response):
        for output in describe_stack_response["Stacks"][0].get("Outputs", []):
            self.set_output(output["OutputKey"], output["OutputValue"])
