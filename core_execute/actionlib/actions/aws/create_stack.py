"""Deploy a Cloudformation stack"""

from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator

import core_logging as log

from core_framework.models import DeploymentDetails, ActionSpec

import core_framework as util

from botocore.exceptions import ClientError

import core_helper.aws as aws

from core_execute.actionlib.action import BaseAction

CAPABILITITES = ["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM"]


class CreateStackActionParams(BaseModel):
    """Parameters for the CreateStackAction"""

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
        description="The name of the stack to create (required)",
    )
    template_url: str = Field(
        ...,
        alias="TemplateUrl",
        description="The URL of the CloudFormation template (required)",
    )
    stack_parameters: dict[str, Any] = Field(
        default_factory=dict,
        alias="StackParameters",
        description="The parameters to pass to the stack (optional)",
    )
    on_failure: str = Field(
        default="DELETE",
        alias="OnFailure",
        description="The action to take on failure (optional)",
    )
    timeout_in_minutes: int = Field(
        default=15,
        alias="TimeoutInMinutes",
        description="The time to wait for the stack to complete (optional)",
    )
    tags: dict[str, str] = Field(
        default_factory=dict,
        alias="Tags",
        description="The tags to apply to the stack (optional)",
    )
    stack_policy: str = Field(
        default="",
        alias="StackPolicy",
        description="A policy statement to use within the stack deployment as needed (optional) (converted to JSON)",
    )


class CreateStackActionSpec(ActionSpec):
    """Generate the action definition"""

    @model_validator(mode="before")
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Validate the parameters for the CreateStackActionSpec"""
        if not (values.get("name") or values.get("Name")):
            values["name"] = "action-aws-createstack-name"
        if not (values.get("kind") or values.get("Kind")):
            values["kind"] = "AWS::CreateStack"
        if not (values.get("depends_on") or values.get("DependsOn")):
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Params")):
            values["params"] = {
                "account": "",
                "region": "",
                "stack_name": "",
                "template_url": "",
                "stack_parameters": {},
                "on_failure": "DELETE",
                "timeout_in_minutes": 15,
                "tags": {},
                "stack_policy": "",
            }
        return values


class CreateStackAction(BaseAction):
    """Create a stack in CloudFormation.

    This action will create a stack in CloudFormation.  The action will wait for the stack to complete before returning.

    The task is typically a "deploy" task.

    Attributes:
        Kind: Use the value: ``AWS::CreateStack``
        Params.Account: The account where CloudFormation is located
        Params.Region: The region where CloudFormation is located
        Params.StackName: The name of the stack to create (required)
        Params.TemplateUrl: The URL of the CloudFormation template (required)
        Params.StackParameters: The parameters to pass to the stack (optional)
        Params.OnFailure: The action to take on failure (optional)
        Params.TimeoutInMinutes: The time to wait for the stack to complete (optional)
        Params.Tags: The tags to apply to the stack (optional)
        Params.StackPolicy: A policy statement to use within the stack deployment as needed (optional) (converted to JSON)

    .. rubric:: ActionSpec:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Name: action-aws-createstack-name
              Kind: "AWS::CreateStack"
              Params:
                Account: "154798051514"
                StackName: "my-applicatin-stack"
                Region: "ap-southeast-1"
                TemplateUrl: "s3://my-bucket/my-template.yaml"
                StackParameters:
                  Build: ver1.0
                Tags:
                  App: "My application"
                TimeoutInMinutes: 15
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
        self.params = CreateStackActionParams(**definition.params)

        if deployment_details.delivered_by:
            self.params.tags["DeliveredBy"] = deployment_details.delivered_by

    def __create_stack(self, cfn_client, stack_id):

        log.trace("Creating new stack '{}'".format(self.params.stack_name))

        args = {
            "StackName": self.params.stack_name,
            "TemplateURL": self.params.template_url,
            "Capabilities": CAPABILITITES,
            "Parameters": aws.transform_stack_parameter_hash(
                self.params.stack_parameters
            ),
            "OnFailure": self.params.OnFailure,
        }
        if self.params.tags:
            args["Tags"] = aws.transform_tag_hash(self.params.tags)
        if self.params.timeout_in_minutes is not None:
            args["TimeoutInMinutes"] = self.params.timeout_in_minutes
        if self.params.StackPolicy != "":
            args["StackPolicyBody"] = util.to_json(self.params.stack_policy)
        cfn_response = cfn_client.create_stack(**args)

        stack_id = cfn_response["StackId"]
        self.set_state("StackId", stack_id)
        self.set_running("Creating new stack '{}'".format(self.params.stack_name))

        log.trace("Stack creation initiated")

    def __update_stack(
        self, cfn_client: Any, stack_id: str, describe_stack_response: dict
    ):
        try:
            log.trace("Updating existing stack '{}'".format(self.params.StackName))

            args = {
                "StackName": stack_id,
                "TemplateURL": self.params.template_url,
                "Capabilities": CAPABILITITES,
                "Parameters": aws.transform_stack_parameter_hash(
                    self.params.stack_parameters or {}
                ),
            }
            if self.params.tags:
                args["Tags"] = aws.transform_tag_hash(self.params.tags)
            if self.params.stack_policy:
                args["StackPolicyBody"] = util.to_json(self.params.stack_policy)

            cfn_client.update_stack(**args)

            self.set_running(
                "Updating existing stack '{}'".format(self.params.stack_name)
            )

            log.trace("Stack update initiated")

        except ClientError as e:
            if "No updates" in e.response["Error"]["Message"]:
                self.set_complete("No changes required")
                self.__save_stack_outputs(describe_stack_response)
            else:
                log.error("Error updating stack: {}", e.response["Error"]["Message"])
                raise

    def _execute(self):

        log.trace("Executing CreateStackAction")

        # Obtain a CloudFormation client
        cfn_client = aws.cfn_client(
            region=self.params.Region,
            role=util.get_provisioning_role_arn(self.params.account),
        )

        # Determine if the stack already exists
        stack_id = None
        try:
            describe_stack_response = cfn_client.describe_stacks(
                StackName=self.params.stack_name
            )
            stack_id = describe_stack_response["Stacks"][0]["StackId"]
            self.set_state("StackId", stack_id)
        except ClientError as e:
            if "does not exist" not in e.response["Error"]["Message"]:
                log.error("Error describing stack: {}", e.response["Error"]["Message"])
                raise

        # Stack exists, attempt an update, else crate a new one
        if stack_id:
            self.__update_stack(cfn_client, stack_id, describe_stack_response)
        else:
            self.__create_stack(cfn_client, stack_id)

        log.trace("CreateStackAction execution completed")

    def _check(self):

        log.trace("Checking CreateStackAction")

        # Obtain a CloudFormation client
        cfn_client = aws.cfn_client(
            region=self.params.region,
            role=util.get_provisioning_role_arn(self.params.account),
        )

        # Describe the stack to get its status
        describe_stack_response = cfn_client.describe_stacks(
            StackName=self.get_state("StackId")
        )
        stack_status = describe_stack_response["Stacks"][0]["StackStatus"]

        failed_keywords = ["FAILED", "ROLLBACK", "DELETE"]
        running_keywords = ["IN_PROGRESS"]

        if any(word in stack_status for word in failed_keywords):
            self.set_failed("Stack status is '{}'".format(stack_status))
        elif any(word in stack_status for word in running_keywords):
            log.debug("Stack status is {}", stack_status)
        else:
            self.set_complete()
            self.__save_stack_outputs(describe_stack_response)

        log.trace("CreateStackAction check completed")

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):

        log.trace("Resolving CreateStackAction")

        self.params.region = self.renderer.render_string(self.params.r, self.context)
        self.params.account = self.renderer.render_string(
            self.params.account, self.context
        )
        self.params.stack_name = self.renderer.render_string(
            self.params.stack_name, self.context
        )
        self.params.template_url = self.renderer.render_string(
            self.params.template_url, self.context
        )
        self.params.on_failure = self.renderer.render_string(
            self.params.on_failure, self.context
        )
        self.params.timeout_in_minutes = self.renderer.render_string(
            self.params.timeout_in_minutes, self.context
        )

        if self.params.stack_parameters:
            for parameter_key, parameter_value in self.params.stack_parameters.items():

                value = self.renderer.render_string(parameter_value, self.context)
                if value == "_NULL_":
                    self.params.stack_parameters.pop(parameter_key)
                    continue

                self.params.stack_parameters[parameter_key] = value

        log.trace("Resolved CreateStackAction")

    def __save_stack_outputs(self, describe_stack_response):
        for output in describe_stack_response["Stacks"][0].get("Outputs", []):
            self.set_output(output["OutputKey"], output["OutputValue"])
