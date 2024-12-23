"""Remove ELB protection so it can be deleted"""
from typing import Any

from core_framework.models import ActionDefinition, DeploymentDetails, ActionParams

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


def generate_template() -> ActionDefinition:
    """Generate the action definition"""

    definition = ActionDefinition(
        Label="action-definition-label",
        Type="AWS::UnprotectELB",
        DependsOn=['put-a-label-here'],
        Params=ActionParams(
            Account="The account to use for the action (required)",
            Region="The region to create the stack in (required)",
            LoadBalancer="The ARN of the load balancer to unprotect (required)",
        ),
        Scope="Based on your deployment details, it one of 'portfolio', 'app', 'branch', or 'build'",
    )

    return definition


class UnprotectELBAction(BaseAction):
    def __init__(
        self,
        definition: ActionDefinition,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        self.account = self.params.Account
        self.region = self.params.Region
        self.load_balancer = self.params.LoadBalancer

    def _execute(self):
        if self.load_balancer != "none":
            elbv2_client = aws.elbv2_client(
                region=self.region, role=util.get_provisioning_role_arn(self.account)
            )

            elbv2_client.modify_load_balancer_attributes(
                LoadBalancerArn=self.load_balancer,
                Attributes=[{"Key": "deletion_protection.enabled", "Value": "false"}],
            )

        self.set_complete()

    def _check(self):
        self.set_complete()

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):
        self.account = self.renderer.render_string(self.account, self.context)
        self.region = self.renderer.render_string(self.region, self.context)
        self.load_balancer = self.renderer.render_string(
            self.load_balancer, self.context
        )
