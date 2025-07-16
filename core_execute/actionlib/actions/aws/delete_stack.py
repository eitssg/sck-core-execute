"""Teardown or delete a CloudFormation stack"""

from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator

import core_logging as log
from botocore.exceptions import ClientError

from core_framework.models import DeploymentDetails, ActionSpec

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


class DeleteStackActionParams(BaseModel):
    """Parameters for the DeleteStackAction"""

    model_config = ConfigDict(populate_by_name=True, validate_assignment=True)

    account: str = Field(
        ...,
        alias="Account",
        description="The account to use for the action (required)",
    )
    region: str = Field(
        ...,
        alias="Region",
        description="The region to create the stack in (required)",
    )
    stack_name: str = Field(
        ...,
        alias="StackName",
        description="The name of the stack to delete (required)",
    )
    success_statuses: list[str] = Field(
        default_factory=list,
        alias="SuccessStatuses",
        description="The stack statuses that indicate success (optional). Defaults to []",
    )


class DeleteStackActionSpec(ActionSpec):
    """Generate the action definition"""

    @model_validator(mode="before")
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Validate the parameters for the DeleteStackActionSpec"""
        if not (values.get("name") or values.get("Name")):
            values["name"] = "action-aws-deletestack-name"
        if not (values.get("kind") or values.get("Kind")):
            values["kind"] = "AWS::DeleteStack"
        if not (values.get("depends_on") or values.get("DependsOn")):
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Params")):
            values["params"] = {
                "account": "",
                "region": "",
                "stack_name": "",
                "success_statuses": [],
            }
        return values


class DeleteStackAction(BaseAction):
    """Delete a CloudFormation stack

    This action will delete a CloudFormation stack.  The action will wait for the deletion to complete before returning.

    This process is typiclly part of a "teardown" task.

    Attributes:
        Kind: Use the value: ``AWS::DeleteStack``
        Params.Account: The account where the stack is located
        Params.Region: The region where the stack is located
        Params.StackName: The name of the stack to delete (required)
        Params.SuccessStatuses: The stack statuses that indicate success (optional).  Defaults to []

    .. rubric:: ActionSpec:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Name: action-aws-deletestack-name
              Kind: "AWS::DeleteStack"
              Params:
                Account: "154798051514"
                Region: "ap-southeast-1"
                StackName: "my-appication-stack-name"
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
        self.params = DeleteStackActionParams(**definition.params)

    def _execute(self):

        log.trace("DeleteStackAction._execute()")

        # Obtain a CloudFormation client
        cfn_client = aws.cfn_client(
            region=self.params.Region,
            role=util.get_provisioning_role_arn(self.params.account),
        )

        # Describe the stack to get its status
        exists = True
        try:
            cfn_response = cfn_client.describe_stacks(StackName=self.params.stack_name)
            stack_status = cfn_response["Stacks"][0]["StackStatus"]
            stack_id = cfn_response["Stacks"][0]["StackId"]
            self.set_state("StackId", stack_id)
            self.set_output("StackStatus", stack_status)
        except ClientError as e:
            if "does not exist" in e.response["Error"]["Message"]:
                log.warning("Stack '{}' does not exist", self.params.stack_name)
                exists = False
            else:
                log.error("Error describing stack '{}': {}", self.params.stack_name, e)
                raise

        if exists is False:
            self.set_output("StackStatus", "DELETE_COMPLETE")
            self.set_complete(
                "Stack '{}' does not exist, it may have been previously deleted".format(
                    self.params.stack_name
                )
            )
        elif stack_status == "DELETE_COMPLETE":
            self.set_output("StackStatus", stack_status)
            self.set_complete("Stack '{}' has been previously deleted")
        else:
            self.set_running("Deleting stack '{}'".format(self.params.stack_name))
            cfn_response = cfn_client.delete_stack(StackName=self.get_state("StackId"))

        log.trace("DeleteStackAction._execute() complete")

    def _check(self):

        log.trace("DeleteStackAction._check()")

        # Obtain a CloudFormation client
        cfn_client = aws.cfn_client(
            region=self.params.Region,
            role=util.get_provisioning_role_arn(self.params.account),
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
                log.warning("Stack '{}' does not exist", self.params.stack_name)
                exists = False
            else:
                log.error("Error describing stack '{}': {}", self.params.stack_name, e)
                raise

        if exists is False or stack_status == "DELETE_COMPLETE":
            self.set_output("StackStatus", "DELETE_COMPLETE")
            self.set_complete()
        elif (
            not self.params.success_statuses
            or stack_status in self.params.success_statuses
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

        self.params.account = self.renderer.render_string(
            self.params.account, self.context
        )
        self.params.region = self.renderer.render_string(
            self.params.region, self.context
        )
        self.params.stack_name = self.renderer.render_string(
            self.params.stack_name, self.context
        )

        log.trace("DeleteStackAction._resolve()")
