"""Delete an image and its associated snapshots"""
from typing import Any

import core_logging as log

from core_framework.models import DeploymentDetails, ActionDefinition, ActionParams

from botocore.exceptions import ClientError

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


def generate_template() -> ActionDefinition:
    """Generate the action definition"""

    definition = ActionDefinition(
        Label="action-definition-label",
        Type="AWS::DeleteImage",
        DependsOn=['put-a-label-here'],
        Params=ActionParams(
            Account="The account to use for the action (required)",
            Region="The region to create the stack in (required)",
            ImageName="The name of the image to delete (required)",
        ),
        Scope="Based on your deployment details, it one of 'portfolio', 'app', 'branch', or 'build'",
    )

    return definition


class DeleteImageAction(BaseAction):
    def __init__(
        self,
        definition: ActionDefinition,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)
        self.account = self.params.Account
        self.image_name = self.params.ImageName
        self.region = self.params.Region

    def _execute(self):  # noqa: C901
        # Obtain an EC2 client
        ec2_client = aws.ec2_client(
            region=self.region, role=util.get_provisioning_role_arn(self.account)
        )

        # Find image (provides image id and snapshot ids)
        log.debug("Finding image with name '{}'", self.image_name)
        response = ec2_client.describe_images(
            Filters=[{"Name": "name", "Values": [self.image_name]}]
        )

        if len(response["Images"]) == 0:
            self.set_complete(
                "Image '{}' does not exist, may have been previously deleted".format(
                    self.image_name
                )
            )
            return

        image_id = response["Images"][0]["ImageId"]

        log.debug("Found image '{}' with id '{}'", self.image_name, image_id)

        # Extract snapshot ids from describe_images response
        snapshot_ids = []
        for block_device_mapping in response["Images"][0]["BlockDeviceMappings"]:
            if "Ebs" not in block_device_mapping:
                continue
            snapshot_ids.append(block_device_mapping["Ebs"]["SnapshotId"])

        log.debug("Image '{}' has snapshots: {}", image_id, snapshot_ids)

        # Deregister image
        self.set_running("Deregistering image '{}'".format(image_id))
        try:
            ec2_client.deregister_image(ImageId=image_id)
        except ClientError as e:
            if e.response["Error"]["Code"] == "InvalidAMIID.Unavailable":
                log.warn(
                    "Failed to deregister image '{}', it may have been previously deleted - {}".format(
                        image_id, e
                    )
                )
            else:
                raise

        # Delete image snapshots
        self.set_running("Deleting snapshots for image '{}'".format(image_id))
        for snapshot_id in snapshot_ids:

            log.debug("Deleting snapshot '{}'", snapshot_id)

            try:
                ec2_client.delete_snapshot(SnapshotId=snapshot_id)
            except ClientError as e:
                if e.response["Error"]["Code"] == "InvalidSnapshot.NotFound":
                    log.warn(
                        "Failed to delete snapshot '{}', it may have been previously deleted - {}",
                        snapshot_id,
                        e,
                    )
                else:
                    raise

        self.set_complete()

    def _check(self):
        self.set_complete()

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):
        self.account = self.renderer.render_string(self.account, self.context)
        self.image_name = self.renderer.render_string(self.image_name, self.context)
        self.region = self.renderer.render_string(self.region, self.context)
