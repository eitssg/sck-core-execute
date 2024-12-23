"""Delete ENIs attached to a security group"""
from typing import Any

import core_logging as log

from core_framework.models import DeploymentDetails, ActionDefinition

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction, ActionParams


# If this account is hyperplane enabled, amazon manages the ENI attachments for you.
ENI_OWNER_HYPERPLANE = "amazon-aws"


def generate_template() -> ActionDefinition:
    """Generate the action definition"""

    definition = ActionDefinition(
        Label="action-definition-label",
        Type="AWS::DeleteSecurityGroupEnis",
        DependsOn=['put-a-label-here'],
        Params=ActionParams(
            Account="The account to use for the action (required)",
            Region="The region to create the stack in (required)",
            SecurityGroupId="The ID of the security group to delete ENIs from (required)",
        ),
        Scope="Based on your deployment details, it one of 'portfolio', 'app', 'branch', or 'build'",
    )

    return definition


class DeleteSecurityGroupEnisAction(BaseAction):
    def __init__(
        self,
        definition: ActionDefinition,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)
        self.account = self.params.Account
        self.region = self.params.Region
        self.security_group_id = self.params.SecurityGroupId

    def _execute(self):
        if self.security_group_id:
            self.set_running(
                "Deleting ENIs attached to security group '{}'".format(
                    self.security_group_id
                )
            )
            self.__detach_enis()
        else:
            self.set_complete("Could not find security group, nothing to do")

    def _check(self):
        self.__detach_enis()

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):
        self.account = self.renderer.render_string(self.account, self.context)
        self.region = self.renderer.render_string(self.region, self.context)
        self.security_group_id = self.renderer.render_string(
            self.security_group_id, self.context
        )

    def __detach_enis(self):

        # Obtain an EC2 client
        ec2_client = aws.ec2_client(
            region=self.region, role=util.get_provisioning_role_arn(self.account)
        )

        # Retrieve security group ENIs
        response = ec2_client.describe_network_interfaces(
            Filters=[{"Name": "group-id", "Values": [self.security_group_id]}]
        )
        network_interfaces = response["NetworkInterfaces"]

        if network_interfaces:
            for network_interface in network_interfaces:
                if (
                    network_interface["Status"] == "in-use"
                    and network_interface["Attachment"]["InstanceOwnerId"]
                    != ENI_OWNER_HYPERPLANE
                ):
                    # Skip this step for hyperplane, we have to wait for AWS to detach the ENI and move to available state.
                    # Detach 'in-use' ENIs
                    log.debug(
                        "Detaching ENI '{}' from security group '{}'",
                        network_interface["NetworkInterfaceId"],
                        self.security_group_id,
                    )
                    ec2_client.detach_network_interface(
                        AttachmentId=network_interface["Attachment"]["AttachmentId"],
                        Force=True,
                    )
                elif network_interface["Status"] == "available":
                    # Delete 'available' ENIs
                    log.debug(
                        "Deleting ENI '{}'", network_interface["NetworkInterfaceId"]
                    )
                    ec2_client.delete_network_interface(
                        NetworkInterfaceId=network_interface["NetworkInterfaceId"]
                    )
        else:
            self.set_complete(
                "Detached and deleted all ENIs from security group '{}'",
                self.security_group_id,
            )
