"""Copy an AMI from one region to another with encryption"""

from typing import Any
import core_logging as log

import core_framework as util

from core_framework.models import ActionDefinition, DeploymentDetails, ActionParams

import core_helper.aws as aws

from core_execute.actionlib.action import BaseAction


def generate_template() -> ActionDefinition:
    """Generate the action definition"""

    definition = ActionDefinition(
        Label="action-definition-label",
        Type="AWS::CopyImage",
        DependsOn=["put-a-label-here"],
        Params=ActionParams(
            Account="The account to use for the action (required)",
            DestinationImageName="The name of the destination image (required)",
            ImageName="The name of the source image (required)",
            KmsKeyArn="The KMS key ARN to use for encryption (required)",
            Region="The region to copy the image to (required)",
            Tags={"any": "The tags to apply to the image (optional)"},
        ),
        Scope="Based on your deployment details, it one of 'portfolio', 'app', 'branch', or 'build'",
    )

    return definition


class CopyImageAction(BaseAction):
    """Copy AMI from one region to another with encryption

    This action will copy an AMI.  The action will wait for the copy to complete before returning.

    Attributes:
        Label: Enter a label to define this action instance
        Type: Use the value: ``AWS::KMS::CopyImage``
        Params.Account: The accoutn where KMS keys are centraly stored
        Params.Region: The region where KMS keys are located
        Params.ImageName: The name of the source image (required)
        Params.KmsKeyArn: The KMS Key ARN to use for encryption (required)

    .. rubric: ActionDefinition:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Label: action-aws-copyimage-label
              Type: "AWS::KMS::CopyImage"
              Params:
                Account: "123456789012"
                Region: "ap-southeast-1"
                ImageName: "My-Image-Name"
                KmsKeyArn: "arn:aws:kms:ap-southeast-1:123456789012:key/your-kms-key-id"
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

        log.trace("Executing CopyImageAction")

        # Obtain an EC2 client
        ec2_client = aws.ec2_client(
            region=self.params.Region,
            role=util.get_provisioning_role_arn(self.params.Account),
        )

        # Find image (provides image id and snapshot ids)
        log.debug("Finding image with name '{}'", self.params.ImageName)
        response = ec2_client.describe_images(
            Filters=[{"Name": "name", "Values": [self.params.ImageName]}]
        )

        if len(response["Images"]) == 0:
            self.set_complete(
                "Could not find image with name '{}'. It may have been previously deleted.".format(
                    self.params.ImageName
                )
            )
            log.warning(
                "Could not find image with name '{}'. It may have been previously deleted.",
                self.params.ImageName,
            )
            return

        image_id = response["Images"][0]["ImageId"]
        log.debug("Found image '{}' with name '{}'", image_id, self.params.ImageName)

        # Encrypt AMI by copying source AMI with encryption option
        self.set_running("Encrypting new image")
        response = ec2_client.copy_image(
            Encrypted=True,
            KmsKeyId=self.params.KmsKeyArn,
            Name=self.params.DestinationImageName,
            SourceImageId=image_id,
            SourceRegion=self.params.Region,
        )

        image_id = response["ImageId"]
        self.set_output("ImageId", image_id)
        self.set_state("ImageId", image_id)

        log.trace("CopyImageAction completed")

    def _check(self):

        log.trace("Checking CopyImageAction")

        # Obtain an EC2 client
        ec2_client = aws.ec2_client(
            region=self.params.Region,
            role=util.provisioning_role_arn(self.params.Account),
        )

        # Wait for image creation to complete / fail
        image_id = self.get_state("ImageId")
        if image_id is None:
            log.error(
                "Internal error - state variable ImageId should have been set during action execution"
            )
            self.set_failed("No image previously created - cannot continue")
            return

        log.debug("Checking availability of copied image '{}'", image_id)
        describe_images_response = ec2_client.describe_images(ImageIds=[image_id])

        if len(describe_images_response["Images"]) == 0:
            self.set_failed("No images found with id '{}'", image_id)
            log.warning("No images found with id '{}'", image_id)
            return

        state = describe_images_response["Images"][0]["State"]

        if state == "available":
            self.set_running("Tagging image '{}'".format(image_id))
            ec2_client.create_tags(Resources=[image_id], Tags=self.tags)

            image_snapshots = self.__get_image_snapshots(describe_images_response)
            self.set_running(
                "Tagging image snapshots: '{}'".format(", ".join(image_snapshots))
            )
            if len(image_snapshots) > 0:
                ec2_client.create_tags(Resources=image_snapshots, Tags=self.tags)

            self.set_complete("Image is in state '{}'".format(state))

        elif state == "pending":
            self.set_running("Image is in state '{}'".format(state))
        else:
            self.set_failed("Image is in state '{}'".format(state))

        log.trace("CopyImageAction check completed")

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):

        log.trace("Resolving CopyImageAction")

        self.params.Account = self.renderer.render_string(
            self.params.Account, self.context
        )
        self.params.ImageName = self.renderer.render_string(
            self.params.ImageName, self.context
        )
        self.params.Region = self.renderer.render_string(
            self.params.Region, self.context
        )

        log.trace("CopyImageAction resolved")

    def __get_image_snapshots(self, describe_images_response):
        snapshots = []
        for mapping in describe_images_response["Images"][0]["BlockDeviceMappings"]:
            if ("Ebs" in mapping) and ("SnapshotId" in mapping["Ebs"]):
                snapshots.append(mapping["Ebs"]["SnapshotId"])
        return snapshots
