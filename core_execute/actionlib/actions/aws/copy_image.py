"""Copy an AMI from one region to another with encryption"""

from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator

import core_logging as log

import core_framework as util

from core_framework.models import ActionSpec, DeploymentDetails

import core_helper.aws as aws

from core_execute.actionlib.action import BaseAction


class CopyImageActionParams(BaseModel):
    """Parameters for the CopyImageAction"""

    model_config = ConfigDict(populate_by_name=True, validate_assignment=True)

    account: str = Field(
        ...,
        alias="Account",
        description="The account to use for the action (required)",
    )
    destination_image_name: str = Field(
        ...,
        alias="DestinationImageName",
        description="The name of the destination image (required)",
    )
    image_name: str = Field(
        ...,
        alias="ImageName",
        description="The name of the source image (required)",
    )
    kms_key_arn: str = Field(
        ...,
        alias="KmsKeyArn",
        description="The KMS key ARN to use for encryption (required)",
    )
    region: str = Field(
        ...,
        alias="Region",
        description="The region to copy the image to (required)",
    )
    tags: dict[str, str] | None = Field(
        default_factory=dict,
        alias="Tags",
        description="The tags to apply to the image (optional)",
    )


class CopyImageActionSpec(ActionSpec):
    """Generate the action definition"""

    @model_validator(mode="before")
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Validate the parameters for the CopyImageActionSpec"""
        if not (values.get("name") or values.get("Name")):
            values["name"] = "action-aws-copyimage-name"
        if not (values.get("kind") or values.get("Kind")):
            values["kind"] = "AWS::CopyImage"
        if not (values.get("depends_on") or values.get("DependsOn")):
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Params")):
            values["params"] = {
                "account": "",
                "destination_image_name": "",
                "image_name": "",
                "kms_key_arn": "",
                "region": "",
                "tags": {},
            }
        return values


class CopyImageAction(BaseAction):
    """Copy AMI from one region to another with encryption

    This action will copy an AMI.  The action will wait for the copy to complete before returning.

    Attributes:
        Name: Enter a name to define this action instance
        Kind: Use the value: ``AWS::KMS::CopyImage``
        Params.Account: The accoutn where KMS keys are centraly stored
        Params.Region: The region where KMS keys are located
        Params.ImageName: The name of the source image (required)
        Params.KmsKeyArn: The KMS Key ARN to use for encryption (required)

    .. rubric: ActionSpec:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Name: action-aws-copyimage-name
              Kind: "AWS::KMS::CopyImage"
              Params:
                Account: "123456789012"
                Region: "ap-southeast-1"
                ImageName: "My-Image-Name"
                KmsKeyArn: "arn:aws:kms:ap-southeast-1:123456789012:key/your-kms-key-id"
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
        self.params = CopyImageActionParams(**definition.params)

        tags = self.params.tags
        if deployment_details.delivered_by:
            tags["DeliveredBy"] = deployment_details.delivered_by

        self.tags = aws.transform_tag_hash(tags)

    def _execute(self):

        log.trace("Executing CopyImageAction")

        # Obtain an EC2 client
        ec2_client = aws.ec2_client(
            region=self.params.region,
            role=util.get_provisioning_role_arn(self.params.account),
        )

        # Find image (provides image id and snapshot ids)
        log.debug("Finding image with name '{}'", self.params.image_name)
        response = ec2_client.describe_images(
            Filters=[{"Name": "name", "Values": [self.params.image_name]}]
        )

        if len(response["Images"]) == 0:
            self.set_complete(
                "Could not find image with name '{}'. It may have been previously deleted.".format(
                    self.params.image_name
                )
            )
            log.warning(
                "Could not find image with name '{}'. It may have been previously deleted.",
                self.params.image_name,
            )
            return

        image_id = response["Images"][0]["ImageId"]
        log.debug("Found image '{}' with name '{}'", image_id, self.params.image_name)

        # Encrypt AMI by copying source AMI with encryption option
        self.set_running("Encrypting new image")
        response = ec2_client.copy_image(
            Encrypted=True,
            KmsKeyId=self.params.kms_key_arn,
            Name=self.params.destination_image_name,
            SourceImageId=image_id,
            SourceRegion=self.params.region,
        )

        image_id = response["ImageId"]
        self.set_output("ImageId", image_id)
        self.set_state("ImageId", image_id)

        log.trace("CopyImageAction completed")

    def _check(self):

        log.trace("Checking CopyImageAction")

        # Obtain an EC2 client
        ec2_client = aws.ec2_client(
            region=self.params.region,
            role=util.provisioning_role_arn(self.params.account),
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

        self.params.account = self.renderer.render_string(
            self.params.account, self.context
        )
        self.params.image_name = self.renderer.render_string(
            self.params.image_name, self.context
        )
        self.params.region = self.renderer.render_string(
            self.params.region, self.context
        )

        log.trace("CopyImageAction resolved")

    def __get_image_snapshots(self, describe_images_response):
        snapshots = []
        for mapping in describe_images_response["Images"][0]["BlockDeviceMappings"]:
            if ("Ebs" in mapping) and ("SnapshotId" in mapping["Ebs"]):
                snapshots.append(mapping["Ebs"]["SnapshotId"])
        return snapshots
