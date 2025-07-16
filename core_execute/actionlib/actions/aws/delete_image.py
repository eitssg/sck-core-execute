"""Delete an image and its associated snapshots"""

from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator

import core_logging as log

from core_framework.models import DeploymentDetails, ActionSpec

from botocore.exceptions import ClientError

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


class DeleteImageActionParams(BaseModel):
    """Parameters for the DeleteImageAction"""

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
    image_name: str = Field(
        ...,
        alias="ImageName",
        description="The name of the image to delete (required)",
    )


class DeleteImageActionSpec(ActionSpec):
    """Generate the action definition"""

    @model_validator(mode="before")
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Validate the parameters for the DeleteImageActionSpec"""
        if not (values.get("name") or values.get("Name")):
            values["name"] = "action-aws-deleteimage-name"
        if not (values.get("kind") or values.get("Kind")):
            values["kind"] = "AWS::DeleteImage"
        if not (values.get("depends_on") or values.get("DependsOn")):
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Params")):
            values["params"] = {
                "account": "",
                "region": "",
                "image_name": "",
            }
        return values


class DeleteImageAction(BaseAction):
    """Delete an image and its associated snapshots

    This action will delete an image and its associated snapshots.  The action will wait for the deletion to complete before returning.

    Attributes:
        Kind: Use the value: ``AWS::DeleteImage``
        Params.Account: The account where the image is located
        Params.Region: The region where the image is located
        Params.ImageName: The name of the image to delete (required)

    .. rubric: ActionSpec:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Name: action-aws-deleteimage-name
              Kind: "AWS::DeleteImage"
              Params:
                Account: "154798051514"
                Region: "ap-southeast-1"
                ImageName: "my-image-name"
              Scope: "build"

    """

    def __init__(
        self,
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        # Validate the parameters
        self.params = DeleteImageActionParams(**definition.params)

    def _execute(self):  # noqa: C901

        log.trace("DeleteImageAction._execute()")

        # Obtain an EC2 client
        ec2_client = aws.ec2_client(
            region=self.params.Region,
            role=util.get_provisioning_role_arn(self.params.account),
        )

        # Find image (provides image id and snapshot ids)
        log.debug("Finding image with name '{}'", self.params.image_name)
        response = ec2_client.describe_images(
            Filters=[{"Name": "name", "Values": [self.params.image_name]}]
        )

        if len(response["Images"]) == 0:
            log.warning("Image '{}' does not exist", self.params.image_name)
            self.set_complete(
                "Image '{}' does not exist, may have been previously deleted".format(
                    self.params.image_name
                )
            )
            return

        image_id = response["Images"][0]["ImageId"]

        log.debug("Found image '{}' with id '{}'", self.params.image_name, image_id)

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
                log.error("Error deregistering image '{}': {}", image_id, e)
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
                    log.error("Error deleting snapshot '{}': {}", snapshot_id, e)
                    raise

        self.set_complete()

        log.trace("DeleteImageAction._execute() complete")

    def _check(self):
        log.trace("DeleteImageAction._check()")

        self.set_complete()

        log.trace("DeleteImageAction._check() complete")

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):

        log.trace("DeleteImageAction._resolve()")

        self.params.account = self.renderer.render_string(
            self.params.account, self.context
        )
        self.params.image_name = self.renderer.render_string(
            self.params.image_name, self.context
        )
        self.params.region = self.renderer.render_string(
            self.params.region, self.context
        )

        log.trace("DeleteImageAction._resolve() complete")
