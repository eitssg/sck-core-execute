from typing import Any

from core_framework.models import ActionDefinition, DeploymentDetails

import core_helper.aws as aws

import core_execute.envinfo as envinfo
from core_execute.actionlib.action import BaseAction


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
                region=self.region, role=envinfo.provisioning_role_arn(self.account)
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
