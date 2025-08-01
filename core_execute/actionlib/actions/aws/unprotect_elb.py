"""Remove ELB deletion protection so it can be deleted"""

from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator, field_validator

import core_logging as log

from core_framework.models import ActionSpec, DeploymentDetails, ActionParams

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


class UnprotectELBActionParams(ActionParams):
    """Parameters for the UnprotectELBAction.

    Contains all configuration needed to remove deletion protection from an
    AWS Elastic Load Balancer (ELB) in the specified account and region.

    Attributes
    ----------
    account : str
        The AWS account ID where the load balancer is located
    region : str
        The AWS region where the load balancer is located
    load_balancer : str
        The ARN of the load balancer to unprotect
        Can be "none" to skip the operation

    Examples
    --------
    Basic ELB unprotection configuration::

        params = UnprotectELBActionParams(
            account="123456789012",
            region="us-east-1",
            load_balancer="arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/my-lb/1234567890"
        )
    """

    load_balancer: str = Field(
        ...,
        alias="LoadBalancer",
        description="The ARN of the load balancer to unprotect (or 'none' to skip)",
    )


class UnprotectELBActionSpec(ActionSpec):
    """Generate the action definition for UnprotectELB.

    Provides a convenience wrapper for creating UnprotectELB actions
    with sensible defaults for common ELB unprotection use cases.

    Examples
    --------
    Creating an ELB unprotection action spec with defaults::

        spec = UnprotectELBActionSpec()
        # Results in action with name "unprotect-elb", kind "unprotect_elb"
        # and template-based default parameters
    """

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Validate and set default parameters for the UnprotectELBActionSpec.

        Provides sensible defaults for action name, kind, scope, and
        basic parameter structure using template variables.

        Parameters
        ----------
        values : dict[str, Any]
            The input values dictionary

        Returns
        -------
        dict[str, Any]
            The values dictionary with defaults applied
        """
        if not (values.get("name") or values.get("Name")):
            values["name"] = "unprotect-elb"
        if not (values.get("kind") or values.get("Kind")):
            values["kind"] = "AWS::UnprotectELB"
        if not (values.get("depends_on") or values.get("DependsOn")):
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Params")):
            values["params"] = {
                "Account": "{{ deployment.account }}",
                "Region": "{{ deployment.region }}",
                "LoadBalancer": "{{ elb.arn }}",
            }
        return values


class UnprotectELBAction(BaseAction):
    """Remove deletion protection from an AWS Elastic Load Balancer.

    This action disables deletion protection on an existing ELB, allowing it to be
    deleted. The action is commonly used before stack deletion or ELB replacement
    operations where deletion protection needs to be temporarily disabled.

    **Key Features:**

    - Remove deletion protection from Application Load Balancers (ALB)
    - Remove deletion protection from Network Load Balancers (NLB)
    - Support for template variables in load balancer ARNs
    - Graceful handling of "none" values to skip operation
    - Comprehensive error handling and state tracking

    **Use Cases:**

    - Prepare ELBs for deletion during stack teardown
    - Temporary removal of protection for maintenance operations
    - Automated cleanup workflows
    - Infrastructure replacement scenarios

    **Action Parameters:**

    :param Account: AWS account ID where the load balancer is located
    :type Account: str
    :param Region: AWS region where the load balancer is located
    :type Region: str
    :param LoadBalancer: ARN of the load balancer to unprotect (or "none" to skip)
    :type LoadBalancer: str

    **Examples:**

    Simple ELB unprotection:

    .. code-block:: yaml

        - name: unprotect-app-lb
          kind: AWS::UnprotectELB
          params:
            Account: "{{ deployment.account }}"
            Region: "{{ deployment.region }}"
            LoadBalancer: "{{ outputs.app_load_balancer.arn }}"

    Conditional unprotection with fallback:

    .. code-block:: yaml

        - name: unprotect-elb-if-exists
          kind: AWS::UnprotectELB
          params:
            Account: "{{ deployment.account }}"
            Region: "{{ deployment.region }}"
            LoadBalancer: "{{ outputs.load_balancer.arn | default('none') }}"

    **Security Considerations:**

    - Requires appropriate ELB modification permissions
    - Only removes deletion protection, does not delete the load balancer
    - Protection can be re-enabled after the operation if needed
    - Operation is logged for audit purposes

    **State Tracking:**

    This action tracks execution state:

    - ``load_balancer_arn`` - The ARN of the load balancer that was unprotected
    - ``status`` - Success/error/skipped status of the operation
    - ``deletion_protection_disabled`` - Boolean indicating if protection was removed
    - ``error_message`` - Details of any errors encountered
    """

    def __init__(
        self,
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        """Initialize the UnprotectELBAction.

        Parameters
        ----------
        definition : ActionSpec
            The action specification containing parameters and configuration
        context : dict[str, Any]
            Template rendering context with deployment variables
        deployment_details : DeploymentDetails
            Deployment context and metadata
        """
        super().__init__(definition, context, deployment_details)

        # Validate the action parameters
        self.params = UnprotectELBActionParams(**definition.params)

    def _execute(self):
        """Execute the ELB unprotection operation.

        Removes deletion protection from the specified load balancer by calling
        the modify_load_balancer_attributes API with deletion_protection.enabled=false.

        The execution process:

        1. Checks if load balancer ARN is "none" and skips if so
        2. Creates ELBv2 client with appropriate IAM role
        3. Calls modify_load_balancer_attributes to disable deletion protection
        4. Records operation results in action state

        Raises
        ------
        Exception
            If ELB API call fails or load balancer doesn't exist

        Notes
        -----
        This method implements the core functionality and should not be
        called directly. Use the action execution framework instead.
        """
        log.trace("UnprotectELBAction._execute()")

        try:
            # Check if load balancer is "none" - skip operation
            if self.params.load_balancer.lower() == "none":
                log.info("Load balancer ARN is 'none' - skipping unprotection operation")
                self.set_state("status", "skipped")
                self.set_state("load_balancer_arn", "none")
                self.set_state("deletion_protection_disabled", False)
                self.set_complete("Skipped unprotection - load balancer ARN is 'none'")
                return

            # Create ELBv2 client
            elbv2_client = aws.elbv2_client(
                region=self.params.region,
                role=util.get_provisioning_role_arn(self.params.account),
            )

            log.debug(f"Removing deletion protection from load balancer: {self.params.load_balancer}")

            # Get current load balancer details for output
            describe_response = elbv2_client.describe_load_balancers(LoadBalancerArns=[self.params.load_balancer])

            if not describe_response.get("LoadBalancers"):
                raise Exception(f"Load balancer not found: {self.params.load_balancer}")

            lb_details = describe_response["LoadBalancers"][0]

            # Remove deletion protection
            elbv2_client.modify_load_balancer_attributes(
                LoadBalancerArn=self.params.load_balancer,
                Attributes=[{"Key": "deletion_protection.enabled", "Value": "false"}],
            )

            # Record successful operation and load balancer details
            self.set_state("status", "success")
            self.set_state("load_balancer_arn", self.params.load_balancer)
            self.set_state("deletion_protection_disabled", True)
            self.set_state("load_balancer_name", lb_details.get("LoadBalancerName"))
            self.set_state("load_balancer_type", lb_details.get("Type"))
            self.set_state("load_balancer_scheme", lb_details.get("Scheme"))
            self.set_state("load_balancer_state", lb_details.get("State", {}).get("Code"))

            success_message = f"Successfully removed deletion protection from load balancer: {self.params.load_balancer}"
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
        """Check operation - verify deletion protection status.

        Checks the current deletion protection status of the load balancer
        to verify that protection has been successfully removed.

        Raises
        ------
        Exception
            If load balancer status check fails

        Notes
        -----
        This method verifies the operation result and should not be
        called directly. Use the action execution framework instead.
        """
        log.trace("UnprotectELBAction._check()")

        try:
            # Skip check if load balancer is "none"
            if self.params.load_balancer.lower() == "none":
                log.debug("Skipping check - load balancer ARN is 'none'")
                self.set_complete("Check skipped - no load balancer to verify")
                return

            # Create ELBv2 client
            elbv2_client = aws.elbv2_client(
                region=self.params.region,
                role=util.get_provisioning_role_arn(self.params.account),
            )

            # Get current load balancer attributes
            response = elbv2_client.describe_load_balancer_attributes(LoadBalancerArn=self.params.load_balancer)

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
        """Unexecute operation - re-enable deletion protection.

        Re-enables deletion protection on the load balancer, reversing
        the unprotection operation performed during execution.

        Notes
        -----
        This operation restores the original protection state.
        If the load balancer no longer exists, the operation is skipped.
        """
        log.trace("UnprotectELBAction._unexecute()")

        try:
            # Skip if load balancer was "none" or operation was skipped
            if self.params.load_balancer.lower() == "none":
                log.debug("Skipping unexecute - load balancer ARN is 'none'")
                return

            deletion_protection_disabled = self.get_state("deletion_protection_disabled")
            if not deletion_protection_disabled:
                log.debug("Skipping unexecute - deletion protection was not disabled")
                return

            # Create ELBv2 client
            elbv2_client = aws.elbv2_client(
                region=self.params.region,
                role=util.get_provisioning_role_arn(self.params.account),
            )

            # Re-enable deletion protection
            elbv2_client.modify_load_balancer_attributes(
                LoadBalancerArn=self.params.load_balancer,
                Attributes=[{"Key": "deletion_protection.enabled", "Value": "true"}],
            )

            log.info(f"Successfully re-enabled deletion protection for load balancer: {self.params.load_balancer}")

        except Exception as e:
            log.warning(f"Failed to re-enable deletion protection during unexecute: {str(e)}")
            # Don't fail the unexecute operation for protection restoration issues

        log.trace("UnprotectELBAction._unexecute() complete")

    def _cancel(self):
        """Cancel operation - not applicable for ELB unprotection.

        ELB attribute modification operations are atomic and complete quickly.
        Cancellation is not supported for this action type.

        Notes
        -----
        This is a no-op method as ELB operations cannot be cancelled.
        """
        log.debug("Cancel requested for ELB unprotection - operation cannot be cancelled")

    def _resolve(self):
        """Resolve template variables and prepare parameters for execution.

        Renders all template variables in the action parameters using the
        provided context. This includes account ID, region, and load balancer ARN.

        **Template Variables Available:**

        - ``deployment.*`` - Deployment context (account, region, environment)
        - ``app.*`` - Application information (name, version, config)
        - ``outputs.*`` - Outputs from previous actions or stack operations
        - ``elb.*`` - ELB-specific context (arn, name, dns_name)
        - ``env.*`` - Environment variables

        Raises
        ------
        Exception
            If template rendering fails or parameter validation errors occur

        Notes
        -----
        This method prepares data for execution and should not be
        called directly. Use the action execution framework instead.
        """
        log.trace("UnprotectELBAction._resolve()")

        try:
            # Render template variables
            self.params.account = self.renderer.render_string(self.params.account, self.context)
            self.params.region = self.renderer.render_string(self.params.region, self.context)
            self.params.load_balancer = self.renderer.render_string(self.params.load_balancer, self.context)

            log.debug(f"Resolved ELB unprotection for load balancer: {self.params.load_balancer}")

        except Exception as e:
            error_message = f"Failed to resolve template variables: {str(e)}"
            log.error(error_message)
            self.set_failed(error_message)

        log.trace("UnprotectELBAction._resolve() complete")

    @classmethod
    def generate_action_spec(cls, **kwargs) -> UnprotectELBActionSpec:
        return UnprotectELBActionSpec(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> UnprotectELBActionParams:
        return UnprotectELBActionParams(**kwargs)
