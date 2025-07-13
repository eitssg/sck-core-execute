"""Create an Image of an EC2 instance"""

from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator

import core_logging as log

from core_framework.models import DeploymentDetails, ActionSpec

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


class CreateImageActionParams(BaseModel):
    """Parameters for the CreateImageAction"""

    model_config = ConfigDict(populate_by_name=True, validate_assignment=True)

    account: str = Field(
        ...,
        alias="Account",
        description="The account to use for the action (required)",
    )
    image_name: str = Field(
        ...,
        alias="ImageName",
        description="The name of the image to create (required)",
    )
    instance_id: str = Field(
        ...,
        alias="InstanceId",
        description="The instance ID to create the image from (required)",
    )
    region: str = Field(
        ...,
        alias="Region",
        description="The region to create the image in (required)",
    )
    tags: dict[str, str] | None = Field(
        default_factory=dict,
        alias="Tags",
        description="The tags to apply to the image (optional)",
    )


class CreateImageActionSpec(ActionSpec):
    """Generate the action definition"""

    @model_validator(mode="before")
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Validate the parameters for the CreateImageActionSpec"""
        if not (values.get("label") or values.get("Label")):
            values["label"] = "action-aws-createimage-label"
        if not (values.get("type") or values.get("Type")):
            values["type"] = "AWS::CreateImage"
        if not (values.get("depends_on") or values.get("DependsOn")):
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Params")):
            values["params"] = {
                "account": "",
                "image_name": "",
                "instance_id": "",
                "region": "",
                "tags": {},
            }

        return values


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
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        self.params = CreateImageActionParams(**definition.params)

        tags = self.params.tags or {}
        if deployment_details.delivered_by:
            tags["DeliveredBy"] = deployment_details.delivered_by

        self.tags = aws.transform_tag_hash(tags)

    def _execute(self):

        log.trace("Executing action")

        # Obtain an EC2 client
        ec2_client = aws.ec2_client(
            region=self.params.region,
            role=util.get_provisioning_role_arn(self.params.account),
        )

        # Create an image
        self.set_running("Creating new image '{}'".format(self.params.image_name))
        response = ec2_client.create_image(
            InstanceId=self.params.instance_id, Name=self.params.image_name
        )
        image_id = response["ImageId"]
        self.set_output("ImageId", image_id)
        self.set_state("ImageId", image_id)

        log.trace("Image created with id '{}'", image_id)

    def _check(self):

        log.trace("Checking action")

        # Obtain an EC2 client
        ec2_client = aws.ec2_client(
            region=self.params.region,
            role=util.get_provisioning_role_arn(self.params.account),
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

        self.params.account = self.renderer.render_string(
            self.params.account, self.context
        )
        self.params.image_name = self.renderer.render_string(
            self.params.image_name, self.context
        )
        self.params.instance_id = self.renderer.render_string(
            self.params.instance_id, self.context
        )
        self.params.region = self.renderer.render_string(
            self.params.region, self.context
        )

        log.trace("Resolved action complete")

    def __get_image_snapshots(self, describe_images_response):
        snapshots = []
        for mapping in describe_images_response["Images"][0]["BlockDeviceMappings"]:
            if ("Ebs" in mapping) and ("SnapshotId" in mapping["Ebs"]):
                snapshots.append(mapping["Ebs"]["SnapshotId"])
        return snapshots
