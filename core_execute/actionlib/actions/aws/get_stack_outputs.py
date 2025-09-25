"""Retrieve CloudFormation stack outputs and expose them to later actions.

Connects with the provisioning role, fetches outputs for a stack, and stores
each key/value in action outputs for template/rendering use downstream.
"""

from typing import Any
from pydantic import Field, model_validator
from botocore.exceptions import ClientError

import core_logging as log

import core_framework as util
import core_helper.aws as aws
from core_framework.models import DeploymentDetails, ActionResource, ActionSpec

from core_execute.actionlib.action import BaseAction


class GetStackOutputsActionSpec(ActionSpec):
    """Parameters for retrieving CloudFormation stack outputs.

    Attributes:
      account: AWS account ID of the stack.
      region: AWS region of the stack.
      stack_name: Name of the CloudFormation stack.
    """

    stack_name: str = Field(
        ...,
        alias="StackName",
        description="The name of the stack to get outputs from (required)",
    )


class GetStackOutputsActionResource(ActionResource):
    """Resource model for GetStackOutputs (normalizes kind/spec)."""

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Normalize incoming values and enforce canonical kind/spec."""
        if not isinstance(values, dict):
            return values

        values.pop("kind", None)
        values.pop("Kind", None)
        values["kind"] = "AWS::GetStackOutputs"

        spec = values.pop("spec", None) or values.pop("Spec", None)
        if isinstance(spec, dict):
            values["spec"] = spec
        elif isinstance(spec, GetStackOutputsActionSpec):
            values["spec"] = spec.model_dump()

        return values


class GetStackOutputsAction(BaseAction[GetStackOutputsActionSpec]):
    """Fetch outputs from a CloudFormation stack and publish them as action outputs.

    Also records stack metadata (ID, status, times). Missing stacks are treated
    as success with a warning and zero outputs.
    """

    def __init__(
        self,
        definition: ActionResource,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        """Initialize the action and validate parameters.

        Args:
          definition: Action resource with metadata/spec.
          context: Rendering context for variables.
          deployment_details: Deployment metadata.

        """
        super().__init__(definition, context, deployment_details)

        # Validate the action parameters
        self.spec = GetStackOutputsActionSpec(**definition.spec)

    def _execute(self):
        """Describe the stack, save outputs to action outputs, and record state.

        Raises:
          ClientError: On CloudFormation errors other than 'stack does not exist'.
        """
        log.trace("GetStackOutputsAction._execute()")

        # Initialize state tracking
        start_time = util.get_current_timestamp()
        self.set_state("start_time", start_time)
        self.set_state("stack_name", self.spec.stack_name)
        self.set_state("account", self.spec.account)
        self.set_state("region", self.spec.region)

        self.set_running(f"Retrieving outputs from CloudFormation stack '{self.spec.stack_name}'")

        # Obtain a CloudFormation client
        cfn_client = aws.cfn_client(
            region=self.spec.region,
            role_arn=util.get_provisioning_role_arn(self.spec.account),
        )

        try:
            describe_stack_response = cfn_client.describe_stacks(StackName=self.spec.stack_name)
            stack = describe_stack_response["Stacks"][0]

            # Extract stack information
            stack_id = stack["StackId"]
            stack_status = stack["StackStatus"]
            creation_time = stack.get("CreationTime", "").isoformat() if stack.get("CreationTime") else ""

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
            self.set_output("stack_name", self.spec.stack_name)
            self.set_output("stack_id", stack_id)
            self.set_output("stack_status", stack_status)
            self.set_output("account", self.spec.account)
            self.set_output("region", self.spec.region)
            self.set_output("outputs_count", outputs_count)
            self.set_output("start_time", start_time)
            self.set_output("completion_time", completion_time)
            self.set_output("status", "success")
            self.set_output(
                "message",
                f"Successfully retrieved {outputs_count} outputs from stack '{self.spec.stack_name}'",
            )

            self.set_complete(f"Retrieved {outputs_count} outputs from stack '{self.spec.stack_name}'")

        except ClientError as e:
            completion_time = util.get_current_timestamp()

            if "does not exist" in e.response["Error"]["Message"]:
                # Stack doesn't exist - treat as success with warning
                self.set_state("completion_time", completion_time)
                self.set_state("status", "completed_not_found")
                self.set_state("outputs_count", 0)

                # Set outputs for non-existent stack
                self.set_output("stack_name", self.spec.stack_name)
                self.set_output("account", self.spec.account)
                self.set_output("region", self.spec.region)
                self.set_output("outputs_count", 0)
                self.set_output("start_time", start_time)
                self.set_output("completion_time", completion_time)
                self.set_output("status", "success")
                self.set_output(
                    "message",
                    f"Stack '{self.spec.stack_name}' does not exist, no outputs retrieved",
                )

                log.warning(
                    "Stack '{}' does not exist, could not retrieve stack outputs",
                    self.spec.stack_name,
                )
                self.set_complete(f"Stack '{self.spec.stack_name}' does not exist")
            else:
                # Other error - set error state
                error_message = str(e)
                self.set_state("error_time", completion_time)
                self.set_state("status", "error")
                self.set_state("error_message", error_message)

                # Set error outputs
                self.set_output("stack_name", self.spec.stack_name)
                self.set_output("account", self.spec.account)
                self.set_output("region", self.spec.region)
                self.set_output("start_time", start_time)
                self.set_output("error_time", completion_time)
                self.set_output("status", "error")
                self.set_output("error_message", error_message)
                self.set_output(
                    "message",
                    f"Error retrieving outputs from stack '{self.spec.stack_name}': {error_message}",
                )

                log.error("Error getting stack outputs: {}", e)
                raise

        log.trace("GetStackOutputsAction._execute() complete")

    def _check(self):
        """Not applicable; operation completes immediately."""
        log.trace("GetStackOutputsAction._check()")

        self.set_failed("Internal error - _check() should not have been called")

        log.trace("GetStackOutputsAction._check() complete")

    def _unexecute(self):
        """No-op; this action is read-only."""
        pass

    def _cancel(self):
        """No-op; action completes immediately and cannot be cancelled."""
        pass

    def _resolve(self):
        """Render account, region, and stack_name from the context."""
        log.trace("GetStackOutputsAction._resolve()")

        self.spec.account = self.renderer.render_string(self.spec.account, self.context)
        self.spec.region = self.renderer.render_string(self.spec.region, self.context)
        self.spec.stack_name = self.renderer.render_string(self.spec.stack_name, self.context)

        log.trace("GetStackOutputsAction._resolve() complete")

    def __save_stack_outputs(self, describe_stack_response):
        """Extract outputs from describe_stacks response and save them.

        Args:
          describe_stack_response: Dict returned by CloudFormation describe_stacks.

        Returns:
          The number of outputs saved.
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
    def generate_action_resource(cls, **kwargs) -> GetStackOutputsActionResource:
        """Factory: create a typed GetStackOutputsActionResource."""
        return GetStackOutputsActionResource(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> GetStackOutputsActionSpec:
        """Factory: create typed GetStackOutputsActionSpec."""
        return GetStackOutputsActionSpec(**kwargs)
