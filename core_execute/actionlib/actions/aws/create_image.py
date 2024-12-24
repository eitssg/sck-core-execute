"""Create an Image of an EC2 instance"""

from typing import Any

import core_logging as log

from core_framework.models import DeploymentDetails, ActionDefinition, ActionParams

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


def generate_template() -> ActionDefinition:
    """Generate the action definition"""

    definition = ActionDefinition(
        Label="action-definition-label",
        Type="AWS::CreateImage",
        DependsOn=["put-a-label-here"],
        Params=ActionParams(
            Account="The account to use for the action (required)",
            ImageName="The name of the image to create (required)",
            InstanceId="The instance ID to create the image from (required)",
            Region="The region to create the image in (required)",
        ),
        Scope="Based on your deployment details, it one of 'portfolio', 'app', 'branch', or 'build'",
    )

    return definition


class CreateImageAction(BaseAction):
    """Create an AMI Image from an EC2 Instance

    This action will create an AMI for an EC2.  It will wait for the operation to complete.

    Attributes:
        Label: Enter a label to define this action instance
        Type:  Use the  value ``AWS::CopyImage``
        Params.Account: The accoutn where KMS keys are centraly stored
        Params.Region: The region where KMS keys are located
        Params.InstanceId: The instance ID to create an image from.
        Params.ImageName: The name of the source image (required)

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Label: action-aws-createimage-label
              Type: "AWS::KMS::CreateImage"
              Params:
                Account: "123456789012"
                Region: "ap-southeast-1"
                InstanceId: "i-1234567890abcdef0"
                ImageName: "My-Image-Name"
              Scope: "build"

    """

    def __init__(
        self,
        definition: ActionDefinition,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        tags = self.params.Tags or {}
        if deployment_details.DeliveredBy:
            tags["DeliveredBy"] = deployment_details.DeliveredBy

        self.tags = aws.transform_tag_hash(tags)

    def _execute(self):

        log.trace("Executing action")

        # Obtain an EC2 client
        ec2_client = aws.ec2_client(
            region=self.params.Region,
            role=util.get_provisioning_role_arn(self.params.Account),
        )

        # Create an image
        self.set_running("Creating new image '{}'".format(self.params.ImageName))
        response = ec2_client.create_image(
            InstanceId=self.params.InstanceId, Name=self.params.ImageName
        )
        image_id = response["ImageId"]
        self.set_output("ImageId", image_id)
        self.set_state("ImageId", image_id)

        log.trace("Image created with id '{}'", image_id)

    def _check(self):

        log.trace("Checking action")

        # Obtain an EC2 client
        ec2_client = aws.ec2_client(
            region=self.params.Region,
            role=util.get_provisioning_role_arn(self.params.Account),
        )

        # Wait for image creation to complete / fail
        image_id = self.get_state("ImageId")
        if image_id is None:
            log.error(
                "Internal error - state variable ImageId should have been set during action execution"
            )
            self.set_failed("No image previously created - cannot continue")
            return

        log.debug("Checking availability of image {}", image_id)
        describe_images_response = ec2_client.describe_images(ImageIds=[image_id])

        if len(describe_images_response["Images"]) == 0:
            log.error("No images found with id '{}'", image_id)
            self.set_failed("Could not find created image '{}'".format(image_id))
            return

        state = describe_images_response["Images"][0]["State"]

        if state == "available":
            ec2_client.create_tags(Resources=[image_id], Tags=self.tags)

            image_snapshots = self.__get_image_snapshots(describe_images_response)
            log.debug("Tagging image snapshots: {}", ", ".join(image_snapshots))
            if len(image_snapshots) > 0:
                ec2_client.create_tags(Resources=image_snapshots, Tags=self.tags)
            self.set_complete()
        elif state == "pending":
            log.debug("Image is in state '{}'", state)
        else:
            self.set_failed("Image '{}' is in state '{}'".format(image_id, state))

        log.trace("Check completed")

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):

        log.trace("Resolving action")

        self.params.Account = self.renderer.render_string(
            self.params.Account, self.context
        )
        self.params.ImageName = self.renderer.render_string(
            self.params.ImageName, self.context
        )
        self.params.InstanceId = self.renderer.render_string(
            self.params.InstanceId, self.context
        )
        self.params.Region = self.renderer.render_string(
            self.params.Region, self.context
        )

        log.trace("Resolved action complete")

    def __get_image_snapshots(self, describe_images_response):
        snapshots = []
        for mapping in describe_images_response["Images"][0]["BlockDeviceMappings"]:
            if ("Ebs" in mapping) and ("SnapshotId" in mapping["Ebs"]):
                snapshots.append(mapping["Ebs"]["SnapshotId"])
        return snapshots
