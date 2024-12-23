"""Deploy a Cloudformation stack"""

from typing import Any

import core_logging as log

from core_framework.models import DeploymentDetails, ActionDefinition, ActionParams

import core_framework as util

from botocore.exceptions import ClientError

import core_helper.aws as aws

from core_execute.actionlib.action import BaseAction


def generate_template() -> ActionDefinition:
    """Generate the action definition"""

    definition = ActionDefinition(
        Label="action-definition-label",
        Type="AWS::CreateStack",
        DependsOn=['put-a-label-here'],
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
        self.template_url = self.params.TemplateUrl
        self.on_failure = self.params.OnFailure or "DELETE"
        self.timeout_in_minutes = self.params.TimeoutInMinutes

        stack_parameters = self.params.StackParameters or {}
        self.stack_parameters = aws.transform_stack_parameter_hash(stack_parameters)

        tags = self.params.Tags or {}
        if deployment_details.DeliveredBy:
            tags["DeliveredBy"] = deployment_details.DeliveredBy

        self.tags = aws.transform_tag_hash(tags)

        self.stack_policy = util.to_json(self.params.StackPolicy)

    def __create_stack(self, cfn_client, stack_id):

        args = {
            "StackName": self.stack_name,
            "TemplateURL": self.template_url,
            "Capabilities": ["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM"],
            "Parameters": (
                self.stack_parameters if self.stack_parameters is not None else []
            ),
            "OnFailure": self.on_failure,
        }
        if self.tags is not None:
            args["Tags"] = self.tags
        if self.timeout_in_minutes is not None:
            args["TimeoutInMinutes"] = self.timeout_in_minutes
        if self.stack_policy != "":
            args["StackPolicyBody"] = self.stack_policy
        cfn_response = cfn_client.create_stack(**args)

        stack_id = cfn_response["StackId"]
        self.set_state("StackId", stack_id)
        self.set_running("Creating new stack '{}'".format(self.stack_name))

    def __update_stack(
        self, cfn_client: Any, stack_id: str, describe_stack_response: dict
    ):
        try:
            args = {
                "StackName": stack_id,
                "TemplateURL": self.template_url,
                "Capabilities": ["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM"],
                "Parameters": (
                    self.stack_parameters if self.stack_parameters is not None else []
                ),
            }
            if self.tags is not None:
                args["Tags"] = self.tags
            if self.stack_policy != "":
                args["StackPolicyBody"] = self.stack_policy

            cfn_client.update_stack(**args)

            self.set_running("Updating existing stack '{}'".format(self.stack_name))

        except ClientError as e:
            if "No updates" in e.response["Error"]["Message"]:
                self.set_complete("No changes required")
                self.__save_stack_outputs(describe_stack_response)
            else:
                raise

    def _execute(self):
        # Obtain a CloudFormation client
        cfn_client = aws.cfn_client(
            region=self.region, role=util.get_provisioning_role_arn(self.account)
        )

        # Determine if the stack already exists
        stack_id = None
        try:
            describe_stack_response = cfn_client.describe_stacks(
                StackName=self.stack_name
            )
            stack_id = describe_stack_response["Stacks"][0]["StackId"]
            self.set_state("StackId", stack_id)
        except ClientError as e:
            if "does not exist" not in e.response["Error"]["Message"]:
                raise

        # Stack exists, attempt an update, else crate a new one
        if stack_id:
            self.__update_stack(cfn_client, stack_id, describe_stack_response)
        else:
            self.__create_stack(cfn_client, stack_id)

    def _check(self):
        # Obtain a CloudFormation client
        cfn_client = aws.cfn_client(
            region=self.region, role=util.get_provisioning_role_arn(self.account)
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

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):
        self.region = self.renderer.render_string(self.region, self.context)
        self.account = self.renderer.render_string(self.account, self.context)
        self.stack_name = self.renderer.render_string(self.stack_name, self.context)
        self.template_url = self.renderer.render_string(self.template_url, self.context)
        self.on_failure = self.renderer.render_string(self.on_failure, self.context)
        self.timeout_in_minutes = self.renderer.render_string(
            self.timeout_in_minutes, self.context
        )

        if self.stack_parameters is not None:
            rendered_stack_parameters = []
            for stack_parameter in self.stack_parameters:
                value = self.renderer.render_string(
                    stack_parameter["ParameterValue"], self.context
                )
                if value == "_NULL_":
                    continue
                rendered_stack_parameters.append(
                    {
                        "ParameterKey": stack_parameter["ParameterKey"],
                        "ParameterValue": value,
                    }
                )
            self.stack_parameters = rendered_stack_parameters

    def __save_stack_outputs(self, describe_stack_response):
        for output in describe_stack_response["Stacks"][0].get("Outputs", []):
            self.set_output(output["OutputKey"], output["OutputValue"])
