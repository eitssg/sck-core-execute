"""Remove ELB protection so it can be deleted"""

from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator

import core_logging as log

from core_framework.models import ActionSpec, DeploymentDetails

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


class UnprotectELBActionParams(BaseModel):
    """Parameters for the UnprotectELBAction"""

    model_config = ConfigDict(populate_by_name=True, validate_assignment=True)

    account: str = Field(
        ..., alias="Account", description="The account to use for the action (required)"
    )
    region: str = Field(
        ..., alias="Region", description="The region to create the stack in (required)"
    )
    load_balancer: str = Field(
        ...,
        alias="LoadBalancer",
        description="The ARN of the load balancer to unprotect (required)",
    )


class UnprotectELBActionSpec(ActionSpec):
    """Generate the action definition"""

    @model_validator(mode="before")
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Validate the parameters for the UnprotectELBActionSpec"""
        if not (values.get("name") or values.get("Name")):
            values["name"] = "action-aws-unprotect-elb-name"
        if not (values.get("kind") or values.get("Kind")):
            values["kind"] = "AWS::UnprotectELB"
        if not (values.get("depends_on") or values.get("DependsOn")):
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Params")):
            values["params"] = {
                "account": "",
                "region": "",
                "load_balancer": "",
            }
        return values


class UnprotectELBAction(BaseAction):
    """Unprotect an ELB so it can be deleted

    This action will unprotect an ELB so it can be deleted.  The action will wait for the protection to be removed before returning.

    Attributes:
        Kind: Use the value: ``AWS::UnprotectELB``
        Params.Account: The account where the ELB is located
        Params.Region: The region where the ELB is located
        Params.LoadBalancer: The ARN of the load balancer to unprotect (required)

    .. rubric: ActionSpec:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Name: action-aws-unprotectelb-name
              Kind: "AWS::UnprotectELB"
              Params:
                Account: "154798051514"
                Region: "ap-southeast-1"
                LoadBalancer: "arn:aws:elasticloadbalancing:ap-southeast-1:154798051514:loadbalancer/app/my-load-balancer/1234567890"
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
        self.params = UnprotectELBActionParams(**definition.params)

    def _execute(self):

        log.trace("UnprotectELBAction._execute()")

        if self.params.load_balancer != "none":
            elbv2_client = aws.elbv2_client(
                region=self.params.region,
                role=util.get_provisioning_role_arn(self.params.account),
            )

            elbv2_client.modify_load_balancer_attributes(
                LoadBalancerArn=self.params.load_balancer,
                Attributes=[{"Key": "deletion_protection.enabled", "Value": "false"}],
            )

        self.set_complete()

        log.trace("UnprotectELBAction._execute()")

    def _check(self):

        log.trace("UnprotectELBAction._check()")

        self.set_complete()

        log.trace("UnprotectELBAction._check()")

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):

        log.trace("UnprotectELBAction._resolve()")

        self.params.account = self.renderer.render_string(
            self.params.account, self.context
        )
        self.params.region = self.renderer.render_string(
            self.params.region, self.context
        )
        self.params.load_balancer = self.renderer.render_string(
            self.params.load_balancer, self.context
        )

        log.trace("UnprotectELBAction._resolve()")
