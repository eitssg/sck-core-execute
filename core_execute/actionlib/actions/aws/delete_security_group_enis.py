"""Delete ENIs attached to a security group"""

from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator

import core_logging as log

from core_framework.models import DeploymentDetails, ActionSpec

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


# If this account is hyperplane enabled, amazon manages the ENI attachments for you.
ENI_OWNER_HYPERPLANE = "amazon-aws"


class DeleteSecurityGroupEnisActionParams(BaseModel):
    """Parameters for the DeleteSecurityGroupEnisAction"""

    model_config = ConfigDict(populate_by_name=True, validate_assignment=True)

    account: str = Field(
        ...,
        alias="Account",
        description="The account to use for the action (required)",
    )
    region: str = Field(
        ...,
        alias="Region",
        description="The region to create the stack in (required)",
    )
    security_group_id: str = Field(
        ...,
        alias="SecurityGroupId",
        description="The ID of the security group to delete ENIs from (required)",
    )


class DeleteSecurityGroupEnisActionSpec(ActionSpec):
    """Generate the action definition"""

    @model_validator(mode="before")
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Validate the parameters for the DeleteSecurityGroupEnisActionSpec"""
        if not (values.get("label") or values.get("Label")):
            values["label"] = "action-aws-deletesecuritygroupenis-label"
        if not (values.get("type") or values.get("Type")):
            values["type"] = "AWS::DeleteSecurityGroupEnis"
        if not (values.get("depends_on") or values.get("DependsOn")):
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Params")):
            values["params"] = {
                "account": "",
                "region": "",
                "security_group_id": "",
            }
        return values


class DeleteSecurityGroupEnisAction(BaseAction):
    """Delete ENIs attached to a security group

    This action will delete ENIs attached to a security group.  The action will wait for the deletion to complete before returning.

    Attributes:
        Type: Use the value: ``AWS::DeleteSecurityGroupEnis``
        Params.Account: The account where the security group is located
        Params.Region: The region where the security group is located
        Params.SecurityGroupId: The ID of the security group to delete ENIs from (required)

    .. rubric: ActionSpec:

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
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        # Validate and set the parameters
        self.params = DeleteSecurityGroupEnisActionParams(**definition.params)

    def _execute(self):

        log.trace("DeleteSecurityGroupEnisAction._execute()")

        if self.params.security_group_id:
            self.set_running(
                "Deleting ENIs attached to security group '{}'".format(
                    self.params.security_group_id
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

        self.params.account = self.renderer.render_string(
            self.params.account, self.context
        )
        self.params.region = self.renderer.render_string(
            self.params.region, self.context
        )
        self.params.security_group_id = self.renderer.render_string(
            self.params.security_group_id, self.context
        )

        log.trace("DeleteSecurityGroupEnisAction._resolve()")

    def __detach_enis(self):

        log.trace("DeleteSecurityGroupEnisAction.__detach_enis()")

        # Obtain an EC2 client
        ec2_client = aws.ec2_client(
            region=self.params.region,
            role=util.get_provisioning_role_arn(self.params.account),
        )

        # Retrieve security group ENIs
        response = ec2_client.describe_network_interfaces(
            Filters=[{"Name": "group-id", "Values": [self.params.security_group_id]}]
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
                        self.params.security_group_id,
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
                self.params.security_group_id,
            )

        log.trace("DeleteSecurityGroupEnisAction.__detach_enis() complete")
