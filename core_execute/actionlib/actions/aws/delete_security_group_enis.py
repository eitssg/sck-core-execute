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
        DependsOn=["put-a-label-here"],
        Params=ActionParams(
            Account="The account to use for the action (required)",
            Region="The region to create the stack in (required)",
            SecurityGroupId="The ID of the security group to delete ENIs from (required)",
        ),
        Scope="Based on your deployment details, it one of 'portfolio', 'app', 'branch', or 'build'",
    )

    return definition


class DeleteSecurityGroupEnisAction(BaseAction):
    """Delete ENIs attached to a security group

    This action will delete ENIs attached to a security group.  The action will wait for the deletion to complete before returning.

    Attributes:
        Type: Use the value: ``AWS::DeleteSecurityGroupEnis``
        Params.Account: The account where the security group is located
        Params.Region: The region where the security group is located
        Params.SecurityGroupId: The ID of the security group to delete ENIs from (required)

    .. rubric: ActionDefinition:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Label: action-aws-deletesecuritygroupenis-label
              Type: "AWS::DeleteSecurityGroupEnis"
              Params:
                Account: "154798051514"
                Region: "ap-southeast-1"
                SecurityGroupId: "security-group-id"
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

        log.trace("DeleteSecurityGroupEnisAction._execute()")

        if self.params.SecurityGroupId:
            self.set_running(
                "Deleting ENIs attached to security group '{}'".format(
                    self.params.SecurityGroupId
                )
            )
            self.__detach_enis()
        else:
            self.set_complete("Could not find security group, nothing to do")

        log.trace("DeleteSecurityGroupEnisAction._execute()")

    def _check(self):
        log.trace("DeleteSecurityGroupEnisAction._check()")

        self.__detach_enis()

        log.trace("DeleteSecurityGroupEnisAction._check()")

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):

        log.trace("DeleteSecurityGroupEnisAction._resolve()")

        self.params.Account = self.renderer.render_string(
            self.params.Account, self.context
        )
        self.params.Region = self.renderer.render_string(
            self.params.Region, self.context
        )
        self.params.SecurityGroupId = self.renderer.render_string(
            self.params.SecurityGroupId, self.context
        )

        log.trace("DeleteSecurityGroupEnisAction._resolve()")

    def __detach_enis(self):

        log.trace("DeleteSecurityGroupEnisAction.__detach_enis()")

        # Obtain an EC2 client
        ec2_client = aws.ec2_client(
            region=self.params.Region,
            role=util.get_provisioning_role_arn(self.params.Account),
        )

        # Retrieve security group ENIs
        response = ec2_client.describe_network_interfaces(
            Filters=[{"Name": "group-id", "Values": [self.params.SecurityGroupId]}]
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
                        self.params.SecurityGroupId,
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
                self.params.SecurityGroupId,
            )

        log.trace("DeleteSecurityGroupEnisAction.__detach_enis() complete")
