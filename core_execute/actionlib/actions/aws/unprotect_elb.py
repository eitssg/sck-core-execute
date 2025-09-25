"""Remove ELB deletion protection so the load balancer can be deleted.

Disables the deletion_protection.enabled attribute on an existing ALB/NLB.
"""

from typing import Any
from pydantic import Field, model_validator

import core_logging as log

from core_framework.models import ActionResource, DeploymentDetails, ActionSpec

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


class UnprotectELBActionSpec(ActionSpec):
    """Parameters for the UnprotectELB action.

    Attributes:
      account: AWS account ID where the load balancer resides.
      region: AWS region of the load balancer.
      load_balancer: ARN of the target load balancer (use "none" to skip).
    """

    load_balancer: str = Field(
        ...,
        alias="LoadBalancer",
        description="The ARN of the load balancer to unprotect (or 'none' to skip)",
    )


class UnprotectELBActionResource(ActionResource):
    """Resource model for UnprotectELB.

    Normalizes inputs and forces kind to 'AWS::UnprotectELB'.
    """

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Normalize incoming values and enforce canonical kind/spec.

        - Forces kind to 'AWS::UnprotectELB'
        - Accepts spec as dict or UnprotectELBActionSpec
        """
        if not isinstance(values, dict):
            return values

        values.pop("kind", None)
        values.pop("Kind", None)
        values["kind"] = "AWS::UnprotectELB"

        spec = values.pop("spec", None) or values.pop("Spec", None)
        if isinstance(spec, dict):
            values["spec"] = spec
        elif isinstance(spec, UnprotectELBActionSpec):
            values["spec"] = spec.model_dump()

        return values


class UnprotectELBAction(BaseAction[UnprotectELBActionSpec]):
    """Disable deletion protection on an AWS Elastic Load Balancer.

    Commonly used before stack teardown or ELB replacement so the LB can be deleted.
    """

    def __init__(
        self,
        definition: ActionResource,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        """Initialize action and validate parameters.

        Args:
          definition: Action resource with metadata and spec.
          context: Rendering context with deployment variables.
          deployment_details: Deployment metadata.

        """
        super().__init__(definition, context, deployment_details)

        # Validate the action parameters
        self.spec = UnprotectELBActionSpec(**definition.spec)

    def _execute(self):
        """Disable deletion protection on the specified load balancer.

        Steps:
          1) Skip if LoadBalancer is "none"
          2) Describe LB (for metadata/state)
          3) Set deletion_protection.enabled = false
          4) Record operation details in action state
        """
        log.trace("UnprotectELBAction._execute()")

        try:
            # Check if load balancer is "none" - skip operation
            if self.spec.load_balancer.lower() == "none":
                log.info("Load balancer ARN is 'none' - skipping unprotection operation")
                self.set_state("status", "skipped")
                self.set_state("load_balancer_arn", "none")
                self.set_state("deletion_protection_disabled", False)
                self.set_complete("Skipped unprotection - load balancer ARN is 'none'")
                return

            # Create ELBv2 client
            elbv2_client = aws.elbv2_client(
                region=self.spec.region,
                role_arn=util.get_provisioning_role_arn(self.spec.account),
            )

            log.debug(f"Removing deletion protection from load balancer: {self.spec.load_balancer}")

            # Get current load balancer details for output
            describe_response = elbv2_client.describe_load_balancers(LoadBalancerArns=[self.spec.load_balancer])

            if not describe_response.get("LoadBalancers"):
                raise Exception(f"Load balancer not found: {self.spec.load_balancer}")

            lb_details = describe_response["LoadBalancers"][0]

            # Remove deletion protection
            elbv2_client.modify_load_balancer_attributes(
                LoadBalancerArn=self.spec.load_balancer,
                Attributes=[{"Key": "deletion_protection.enabled", "Value": "false"}],
            )

            # Record successful operation and load balancer details
            self.set_state("status", "success")
            self.set_state("load_balancer_arn", self.spec.load_balancer)
            self.set_state("deletion_protection_disabled", True)
            self.set_state("load_balancer_name", lb_details.get("LoadBalancerName"))
            self.set_state("load_balancer_type", lb_details.get("Type"))
            self.set_state("load_balancer_scheme", lb_details.get("Scheme"))
            self.set_state("load_balancer_state", lb_details.get("State", {}).get("Code"))

            success_message = f"Successfully removed deletion protection from load balancer: {self.spec.load_balancer}"
            log.info(success_message)
            self.set_complete(success_message)

        except Exception as e:
            error_message = f"Failed to remove deletion protection from load balancer: {str(e)}"
            log.error(error_message)
            self.set_state("status", "error")
            self.set_state("error_message", error_message)
            self.set_state("deletion_protection_disabled", False)
            self.set_failed(error_message)

        log.trace("UnprotectELBAction._execute() complete")

    def _check(self):
        """Verify that deletion protection is disabled on the load balancer."""
        log.trace("UnprotectELBAction._check()")

        try:
            # Skip check if load balancer is "none"
            if self.spec.load_balancer.lower() == "none":
                log.debug("Skipping check - load balancer ARN is 'none'")
                self.set_complete("Check skipped - no load balancer to verify")
                return

            # Create ELBv2 client
            elbv2_client = aws.elbv2_client(
                region=self.spec.region,
                role_arn=util.get_provisioning_role_arn(self.spec.account),
            )

            # Get current load balancer attributes
            response = elbv2_client.describe_load_balancer_attributes(LoadBalancerArn=self.spec.load_balancer)

            # Check deletion protection status
            deletion_protection_enabled = False
            for attr in response.get("Attributes", []):
                if attr["Key"] == "deletion_protection.enabled":
                    deletion_protection_enabled = attr["Value"].lower() == "true"
                    break

            if deletion_protection_enabled:
                error_message = "Deletion protection is still enabled on the load balancer"
                log.error(error_message)
                self.set_failed(error_message)
            else:
                success_message = "Verified deletion protection is disabled"
                log.info(success_message)
                self.set_complete(success_message)

        except Exception as e:
            error_message = f"Failed to check load balancer deletion protection status: {str(e)}"
            log.error(error_message)
            self.set_failed(error_message)

        log.trace("UnprotectELBAction._check() complete")

    def _unexecute(self):
        """Re-enable deletion protection (best effort)."""
        log.trace("UnprotectELBAction._unexecute()")

        try:
            # Skip if load balancer was "none" or operation was skipped
            if self.spec.load_balancer.lower() == "none":
                log.debug("Skipping unexecute - load balancer ARN is 'none'")
                return

            deletion_protection_disabled = self.get_state("deletion_protection_disabled")
            if not deletion_protection_disabled:
                log.debug("Skipping unexecute - deletion protection was not disabled")
                return

            # Create ELBv2 client
            elbv2_client = aws.elbv2_client(
                region=self.spec.region,
                role_arn=util.get_provisioning_role_arn(self.spec.account),
            )

            # Re-enable deletion protection
            elbv2_client.modify_load_balancer_attributes(
                LoadBalancerArn=self.spec.load_balancer,
                Attributes=[{"Key": "deletion_protection.enabled", "Value": "true"}],
            )

            log.info(f"Successfully re-enabled deletion protection for load balancer: {self.spec.load_balancer}")

        except Exception as e:
            log.warning(f"Failed to re-enable deletion protection during unexecute: {str(e)}")
            # Don't fail the unexecute operation for protection restoration issues

        log.trace("UnprotectELBAction._unexecute() complete")

    def _cancel(self):
        """No-op; ELB attribute modification cannot be cancelled."""
        log.debug("Cancel requested for ELB unprotection - operation cannot be cancelled")

    def _resolve(self):
        """Render templates and prepare parameters for execution."""
        log.trace("UnprotectELBAction._resolve()")

        try:
            # Render template variables
            self.spec.account = self.renderer.render_string(self.spec.account, self.context)
            self.spec.region = self.renderer.render_string(self.spec.region, self.context)
            self.spec.load_balancer = self.renderer.render_string(self.spec.load_balancer, self.context)

            log.debug(f"Resolved ELB unprotection for load balancer: {self.spec.load_balancer}")

        except Exception as e:
            error_message = f"Failed to resolve template variables: {str(e)}"
            log.error(error_message)
            self.set_failed(error_message)

        log.trace("UnprotectELBAction._resolve() complete")

    @classmethod
    def generate_action_resource(cls, **kwargs) -> UnprotectELBActionResource:
        """Factory: create a typed UnprotectELBActionResource."""
        return UnprotectELBActionResource(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> UnprotectELBActionSpec:
        """Factory: create typed UnprotectELBActionSpec."""
        return UnprotectELBActionSpec(**kwargs)
