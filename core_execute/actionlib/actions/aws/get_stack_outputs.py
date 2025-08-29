"""Get the outputs of a CloudFormation stack action for Core Execute automation platform."""

from typing import Any
from pydantic import Field, model_validator
from botocore.exceptions import ClientError

import core_logging as log

import core_framework as util
import core_helper.aws as aws
from core_framework.models import DeploymentDetails, ActionSpec, ActionParams

from core_execute.actionlib.action import BaseAction


class GetStackOutputsActionParams(ActionParams):
    """
    Parameters for the GetStackOutputsAction.

    Attributes
    ----------
    account : str
        The AWS account ID where the CloudFormation stack is located.
    region : str
        The AWS region where the CloudFormation stack is located.
    stack_name : str
        The name of the CloudFormation stack to retrieve outputs from.
    """

    stack_name: str = Field(
        ...,
        alias="StackName",
        description="The name of the stack to get outputs from (required)",
    )


class GetStackOutputsActionSpec(ActionSpec):
    """
    Action specification for the GetStackOutputs action.

    Provides validation and default values for GetStackOutputs action definitions.
    """

    @model_validator(mode="before")
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """
        Validate and set default parameters for the GetStackOutputsActionSpec.

        :param values: Input values dictionary.
        :type values: dict[str, Any]
        :return: Validated values with defaults applied.
        :rtype: dict[str, Any]
        """
        if not (values.get("name") or values.get("Name")):
            values["name"] = "action-aws-getstackoutputs-name"
        if not (values.get("kind") or values.get("Kind")):
            values["kind"] = "AWS::GetStackOutputs"
        if not values.get(
            "depends_on", values.get("DependsOn")
        ):  # arrays are falsy if empty
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Spec")):
            values["params"] = {
                "account": "",
                "region": "",
                "stack_name": "",
            }
        return values


class GetStackOutputsAction(BaseAction):
    """
    Retrieve the outputs of a CloudFormation stack.

    This action retrieves the outputs of a CloudFormation stack and makes them
    available in the action context for use by subsequent actions or Jinja2
    template rendering.

    The outputs are stored using the ``set_output()`` method and can be accessed
    using the ``get_output()`` method or through Jinja2 template variables.

    Attributes
    ----------
    params : GetStackOutputsActionParams
        Validated parameters for the action.

    Parameters
    ----------
    Kind : str
        Use the value: ``AWS::GetStackOutputs``
    Spec.Account : str
        The AWS account where the stack is located
    Spec.Region : str
        The AWS region where the stack is located
    Spec.StackName : str
        The name of the stack to get outputs from (required)

    Examples
    --------
    ActionSpec YAML configuration:

    .. code-block:: yaml

        - Name: action-aws-getstackoutputs-name
          Kind: "AWS::GetStackOutputs"
          Spec:
            Account: "154798051514"
            StackName: "my-applications-stack"
            Region: "ap-southeast-1"
          Scope: "build"

    Notes
    -----
    If the specified stack does not exist, the action will complete successfully
    with a warning, but no outputs will be available.

    The stack outputs are stored in the action context and can be referenced
    in subsequent actions using Jinja2 template syntax.
    """

    def __init__(
        self,
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        """
        Initialize the GetStackOutputsAction.

        :param definition: The action specification definition.
        :type definition: ActionSpec
        :param context: Execution context for variable resolution.
        :type context: dict[str, Any]
        :param deployment_details: Details about the current deployment.
        :type deployment_details: DeploymentDetails
        :raises ValidationError: If action parameters are invalid.
        """
        super().__init__(definition, context, deployment_details)

        # Validate the action parameters
        self.params = GetStackOutputsActionParams(**definition.params)

    def _execute(self):
        """
        Execute the stack outputs retrieval operation.

        Connects to CloudFormation using the provisioning role and retrieves
        the outputs from the specified stack. Stores each output as an action
        output for use by subsequent actions.

        :raises ClientError: If CloudFormation operations fail (except for non-existent stacks).
        """
        log.trace("GetStackOutputsAction._execute()")

        # Initialize state tracking
        start_time = util.get_current_timestamp()
        self.set_state("start_time", start_time)
        self.set_state("stack_name", self.params.stack_name)
        self.set_state("account", self.params.account)
        self.set_state("region", self.params.region)

        self.set_running(
            f"Retrieving outputs from CloudFormation stack '{self.params.stack_name}'"
        )

        # Obtain a CloudFormation client
        cfn_client = aws.cfn_client(
            region=self.params.region,
            role=util.get_provisioning_role_arn(self.params.account),
        )

        try:
            describe_stack_response = cfn_client.describe_stacks(
                StackName=self.params.stack_name
            )
            stack = describe_stack_response["Stacks"][0]

            # Extract stack information
            stack_id = stack["StackId"]
            stack_status = stack["StackStatus"]
            creation_time = (
                stack.get("CreationTime", "").isoformat()
                if stack.get("CreationTime")
                else ""
            )

            # Save comprehensive state
            completion_time = util.get_current_timestamp()
            self.set_state("stack_id", stack_id)
            self.set_state("stack_status", stack_status)
            self.set_state("creation_time", creation_time)
            self.set_state("completion_time", completion_time)
            self.set_state("status", "completed")

            # Save stack outputs
            outputs_count = self.__save_stack_outputs(describe_stack_response)
            self.set_state("outputs_count", outputs_count)

            # Set comprehensive action outputs
            self.set_output("stack_name", self.params.stack_name)
            self.set_output("stack_id", stack_id)
            self.set_output("stack_status", stack_status)
            self.set_output("account", self.params.account)
            self.set_output("region", self.params.region)
            self.set_output("outputs_count", outputs_count)
            self.set_output("start_time", start_time)
            self.set_output("completion_time", completion_time)
            self.set_output("status", "success")
            self.set_output(
                "message",
                f"Successfully retrieved {outputs_count} outputs from stack '{self.params.stack_name}'",
            )

            self.set_complete(
                f"Retrieved {outputs_count} outputs from stack '{self.params.stack_name}'"
            )

        except ClientError as e:
            completion_time = util.get_current_timestamp()

            if "does not exist" in e.response["Error"]["Message"]:
                # Stack doesn't exist - treat as success with warning
                self.set_state("completion_time", completion_time)
                self.set_state("status", "completed_not_found")
                self.set_state("outputs_count", 0)

                # Set outputs for non-existent stack
                self.set_output("stack_name", self.params.stack_name)
                self.set_output("account", self.params.account)
                self.set_output("region", self.params.region)
                self.set_output("outputs_count", 0)
                self.set_output("start_time", start_time)
                self.set_output("completion_time", completion_time)
                self.set_output("status", "success")
                self.set_output(
                    "message",
                    f"Stack '{self.params.stack_name}' does not exist, no outputs retrieved",
                )

                log.warning(
                    "Stack '{}' does not exist, could not retrieve stack outputs",
                    self.params.stack_name,
                )
                self.set_complete(f"Stack '{self.params.stack_name}' does not exist")
            else:
                # Other error - set error state
                error_message = str(e)
                self.set_state("error_time", completion_time)
                self.set_state("status", "error")
                self.set_state("error_message", error_message)

                # Set error outputs
                self.set_output("stack_name", self.params.stack_name)
                self.set_output("account", self.params.account)
                self.set_output("region", self.params.region)
                self.set_output("start_time", start_time)
                self.set_output("error_time", completion_time)
                self.set_output("status", "error")
                self.set_output("error_message", error_message)
                self.set_output(
                    "message",
                    f"Error retrieving outputs from stack '{self.params.stack_name}': {error_message}",
                )

                log.error("Error getting stack outputs: {}", e)
                raise

        log.trace("GetStackOutputsAction._execute() complete")

    def _check(self):
        """
        Check the status of the stack outputs operation.

        This method should not be called for GetStackOutputs actions as the
        operation completes immediately. If called, it indicates an internal error.
        """
        log.trace("GetStackOutputsAction._check()")

        self.set_failed("Internal error - _check() should not have been called")

        log.trace("GetStackOutputsAction._check() complete")

    def _unexecute(self):
        """
        Reverse the stack outputs operation.

        This operation cannot be reversed as it only reads data.
        This method is provided for interface compliance but performs no action.
        """
        pass

    def _cancel(self):
        """
        Cancel the stack outputs operation.

        This operation cannot be cancelled as it completes immediately.
        This method is provided for interface compliance but performs no action.
        """
        pass

    def _resolve(self):
        """
        Resolve template variables in action parameters.

        Uses the renderer to substitute variables in the account, region,
        and stack_name parameters using the current execution context.
        """
        log.trace("GetStackOutputsAction._resolve()")

        self.params.account = self.renderer.render_string(
            self.params.account, self.context
        )
        self.params.region = self.renderer.render_string(
            self.params.region, self.context
        )
        self.params.stack_name = self.renderer.render_string(
            self.params.stack_name, self.context
        )

        log.trace("GetStackOutputsAction._resolve() complete")

    def __save_stack_outputs(self, describe_stack_response):
        """
        Extract and save stack outputs from the CloudFormation response.

        Iterates through the stack outputs in the describe_stacks response
        and stores each output key-value pair using the set_output method.

        :param describe_stack_response: Response from CloudFormation describe_stacks API call.
        :type describe_stack_response: dict
        :return: Number of outputs saved.
        :rtype: int
        """
        log.trace("GetStackOutputsAction.__save_stack_outputs()")

        outputs = describe_stack_response["Stacks"][0].get("Outputs", [])
        outputs_count = 0

        for output in outputs:
            output_key = output["OutputKey"]
            output_value = output["OutputValue"]
            output_description = output.get("Description", "")

            # Save the actual output value
            self.set_output(output_key, output_value)

            # Also save metadata about each output
            self.set_output(f"{output_key}_description", output_description)

            outputs_count += 1

            log.debug("Saved stack output: {} = {}", output_key, output_value)

        log.debug("Saved {} stack outputs", outputs_count)
        log.trace("GetStackOutputsAction.__save_stack_outputs() complete")

        return outputs_count

    @classmethod
    def generate_action_spec(cls, **kwargs) -> GetStackOutputsActionSpec:
        return GetStackOutputsActionSpec(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> GetStackOutputsActionParams:
        return GetStackOutputsActionParams(**kwargs)
