"""Deploy a Cloudformation stack"""

from typing import Any

import core_logging as log

from core_framework.models import DeploymentDetails, ActionDefinition, ActionParams

import core_framework as util

from botocore.exceptions import ClientError

import core_helper.aws as aws

from core_execute.actionlib.action import BaseAction

CAPABILITITES = ["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM"]


def generate_template() -> ActionDefinition:
    """Generate the action definition"""

    definition = ActionDefinition(
        Label="action-definition-label",
        Type="AWS::CreateStack",
        DependsOn=["put-a-label-here"],
        Params=ActionParams(
            Account="The account to use for the action (required)",
            Region="The region to create the stack in (required)",
            StackName="The name of the stack to create (required)",
            TemplateUrl="The URL of the CloudFormation template (required)",
            StackParameters={"any": "The parameters to pass to the stack (optional)"},
            OnFailure="The action to take on failure (optional)",
            TimeoutInMinutes=15,
            Tags={"any": "The tags to apply to the stack (optional)"},
            StackPolicy={
                "any": "A policy statement to use within the stack deployment as needed (optional) (converted to JSON)"
            },
        ),
        Scope="Based on your deployment details, it one of 'portfolio', 'app', 'branch', or 'build'",
    )

    return definition


class CreateStackAction(BaseAction):
    """Create a stack in CloudFormation.

    This action will create a stack in CloudFormation.  The action will wait for the stack to complete before returning.

    The task is typically a "deploy" task.

    Attributes:
        Type: Use the value: ``AWS::CreateStack``
        Params.Account: The account where CloudFormation is located
        Params.Region: The region where CloudFormation is located
        Params.StackName: The name of the stack to create (required)
        Params.TemplateUrl: The URL of the CloudFormation template (required)
        Params.StackParameters: The parameters to pass to the stack (optional)
        Params.OnFailure: The action to take on failure (optional)
        Params.TimeoutInMinutes: The time to wait for the stack to complete (optional)
        Params.Tags: The tags to apply to the stack (optional)
        Params.StackPolicy: A policy statement to use within the stack deployment as needed (optional) (converted to JSON)

    .. rubric:: ActionDefinition:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Label: action-aws-createstack-label
              Type: "AWS::CreateStack"
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
        definition: ActionDefinition,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        if self.params.OnFailure is None:
            self.params.OnFailure = "DELETE"

        if self.params.TimeoutInMinutes is None:
            self.params.StackParameters = {}

        if self.params.Tags is None:
            self.params.Tags = {}

        if deployment_details.DeliveredBy:
            self.params.Tags["DeliveredBy"] = deployment_details.DeliveredBy

    def __create_stack(self, cfn_client, stack_id):

        log.trace("Creating new stack '{}'".format(self.params.StackName))

        args = {
            "StackName": self.params.StackName,
            "TemplateURL": self.params.TemplateUrl,
            "Capabilities": CAPABILITITES,
            "Parameters": aws.transform_stack_parameter_hash(
                self.params.StackParameters
            ),
            "OnFailure": self.params.OnFailure,
        }
        if self.params.Tags:
            args["Tags"] = aws.transform_tag_hash(self.params.Tags)
        if self.params.TimeoutInMinutes is not None:
            args["TimeoutInMinutes"] = self.params.TimeoutInMinutes
        if self.params.StackPolicy != "":
            args["StackPolicyBody"] = util.to_json(self.params.StackPolicy)
        cfn_response = cfn_client.create_stack(**args)

        stack_id = cfn_response["StackId"]
        self.set_state("StackId", stack_id)
        self.set_running("Creating new stack '{}'".format(self.params.StackName))

        log.trace("Stack creation initiated")

    def __update_stack(
        self, cfn_client: Any, stack_id: str, describe_stack_response: dict
    ):
        try:
            log.trace("Updating existing stack '{}'".format(self.params.StackName))

            args = {
                "StackName": stack_id,
                "TemplateURL": self.params.TemplateUrl,
                "Capabilities": CAPABILITITES,
                "Parameters": aws.transform_stack_parameter_hash(
                    self.params.StackParameters or {}
                ),
            }
            if self.params.Tags:
                args["Tags"] = aws.transform_tag_hash(self.params.Tags)
            if self.params.StackPolicy:
                args["StackPolicyBody"] = util.to_json(self.params.StackPolicy)

            cfn_client.update_stack(**args)

            self.set_running(
                "Updating existing stack '{}'".format(self.params.StackName)
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
            role=util.get_provisioning_role_arn(self.params.Account),
        )

        # Determine if the stack already exists
        stack_id = None
        try:
            describe_stack_response = cfn_client.describe_stacks(
                StackName=self.params.StackName
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
            region=self.params.Region,
            role=util.get_provisioning_role_arn(self.params.Account),
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

        self.params.Region = self.renderer.render_string(
            self.params.Region, self.context
        )
        self.params.Account = self.renderer.render_string(
            self.params.Account, self.context
        )
        self.params.StackName = self.renderer.render_string(
            self.params.StackName, self.context
        )
        self.params.TemplateUrl = self.renderer.render_string(
            self.params.TemplateUrl, self.context
        )
        self.params.OnFailure = self.renderer.render_string(
            self.params.OnFailure, self.context
        )
        self.params.TimeoutInMinutes = self.renderer.render_string(
            self.params.TimeoutInMinutes, self.context
        )

        if self.params.StackParameters:
            for parameter_key, parameter_value in self.params.StackParameters.items():

                value = self.renderer.render_string(parameter_value, self.context)
                if value == "_NULL_":
                    self.params.StackParameters.pop(parameter_key)
                    continue

                self.params.StackParameters[parameter_key] = value

        log.trace("Resolved CreateStackAction")

    def __save_stack_outputs(self, describe_stack_response):
        for output in describe_stack_response["Stacks"][0].get("Outputs", []):
            self.set_output(output["OutputKey"], output["OutputValue"])
