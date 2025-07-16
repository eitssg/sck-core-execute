"""Get the outputs of a CloudFormation stack"""

from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator

from botocore.exceptions import ClientError

import core_logging as log

from core_framework.models import DeploymentDetails, ActionSpec

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


class GetStackOutputsActionParams(BaseModel):
    """Parameters for the GetStackOutputsAction"""

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
        description="The name of the stack to get outputs from (required)",
    )


class GetStackOutputsActionSpec(ActionSpec):
    """Generate the action definition"""

    @model_validator(mode="before")
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Validate the parameters for the GetStackOutputsActionSpec"""
        if not (values.get("name") or values.get("Name")):
            values["name"] = "action-aws-getstackoutputs-name"
        if not (values.get("kind") or values.get("Kind")):
            values["kind"] = "AWS::GetStackOutputs"
        if not (values.get("depends_on") or values.get("DependsOn")):
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Params")):
            values["params"] = {
                "account": "",
                "region": "",
                "stack_name": "",
            }
        return values


class GetStackOutputsAction(BaseAction):
    """Retrieve the Stake Outputs of a CloudFormation stack

    This action will retrieve the outputs of a CloudFormation stack.  The action will wait for the retrieval to complete before returning.

    If you are wondering about where the outputs are stored, they are stored in the action context.  You can access them using the `get_output()` method.

    when running an action for the currnent state or ... in Jinja2 rendering.

    Attributes:
        Kind: Use the value: ``AWS::GetStackOutputs``
        Params.Account: The account where the stack is located
        Params.Region: The region where the stack is located
        Params.StackName: The name of the stack to get outputs from (required)

    .. rubric: ActionSpec:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Name: action-aws-getstackoutputs-name
              Kind: "AWS::GetStackOutputs"
              Params:
                Account: "154798051514"
                StackName: "my-applications-stack"
                Region: "ap-southeast-1"
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
        self.params = GetStackOutputsActionParams(**definition.params)

    def _execute(self):

        log.trace("GetStackOutputsAction._execute()")

        # Obtain a CloudFormation client
        cfn_client = aws.cfn_client(
            region=self.params.Region,
            role=util.get_provisioning_role_arn(self.params.Account),
        )

        try:
            describe_stack_response = cfn_client.describe_stacks(
                StackName=self.params.StackName
            )
            stack_id = describe_stack_response["Stacks"][0]["StackId"]
            self.__save_stack_outputs(describe_stack_response)
            self.set_state("StackId", stack_id)
            self.set_complete()
        except ClientError as e:
            if "does not exist" in e.response["Error"]["Message"]:
                log.warning(
                    "Stack '{}' does not exist, could not retrieve stack outputs",
                    self.params.StackName,
                )
                self.set_complete()
            else:
                log.error("Error getting stack outputs: {}", e)
                raise

        log.trace("GetStackOutputsAction._execute() complete")

    def _check(self):
        log.trace("GetStackOutputsAction._check()")

        self.set_failed("Internal error - _check() should not have been called")

        log.trace("GetStackOutputsAction._check() complete")

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):

        log.trace("GetStackOutputsAction._resolve()")

        self.params.Account = self.renderer.render_string(
            self.params.Account, self.context
        )
        self.params.Region = self.renderer.render_string(
            self.params.Region, self.context
        )
        self.params.StackName = self.renderer.render_string(
            self.params.StackName, self.context
        )

        log.trace("GetStackOutputsAction._resolve()")

    def __save_stack_outputs(self, describe_stack_response):

        log.trace("GetStackOutputsAction.__save_stack_outputs()")

        for output in describe_stack_response["Stacks"][0].get("Outputs", []):
            self.set_output(output["OutputKey"], output["OutputValue"])

        log.trace("GetStackOutputsAction.__save_stack_outputs() complete")
