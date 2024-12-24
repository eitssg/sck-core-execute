"""Remove ELB protection so it can be deleted"""

from typing import Any

import core_logging as log

from core_framework.models import ActionDefinition, DeploymentDetails, ActionParams

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


def generate_template() -> ActionDefinition:
    """Generate the action definition"""

    definition = ActionDefinition(
        Label="action-definition-label",
        Type="AWS::UnprotectELB",
        DependsOn=["put-a-label-here"],
        Params=ActionParams(
            Account="The account to use for the action (required)",
            Region="The region to create the stack in (required)",
            LoadBalancer="The ARN of the load balancer to unprotect (required)",
        ),
        Scope="Based on your deployment details, it one of 'portfolio', 'app', 'branch', or 'build'",
    )

    return definition


class UnprotectELBAction(BaseAction):
    """Unprotect an ELB so it can be deleted

    This action will unprotect an ELB so it can be deleted.  The action will wait for the protection to be removed before returning.

    Attributes:
        Type: Use the value: ``AWS::UnprotectELB``
        Params.Account: The account where the ELB is located
        Params.Region: The region where the ELB is located
        Params.LoadBalancer: The ARN of the load balancer to unprotect (required)

    .. rubric: ActionDefinition:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Label: action-aws-unprotectelb-label
              Type: "AWS::UnprotectELB"
              Params:
                Account: "154798051514"
                Region: "ap-southeast-1"
                LoadBalancer: "arn:aws:elasticloadbalancing:ap-southeast-1:154798051514:loadbalancer/app/my-load-balancer/1234567890"
              Scope: "build"

    """

    def __init__(
        self,
        definition: ActionDefinition,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

    def _execute(self):

        log.trace("UnprotectELBAction._execute()")

        if self.params.LoadBalancer != "none":
            elbv2_client = aws.elbv2_client(
                region=self.params.Region,
                role=util.get_provisioning_role_arn(self.params.Account),
            )

            elbv2_client.modify_load_balancer_attributes(
                LoadBalancerArn=self.params.LoadBalancer,
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

        self.params.Account = self.renderer.render_string(
            self.params.Account, self.context
        )
        self.params.Region = self.renderer.render_string(
            self.params.Region, self.context
        )
        self.params.LoadBalancer = self.renderer.render_string(
            self.params.LoadBalancer, self.context
        )

        log.trace("UnprotectELBAction._resolve()")
