"""Copy an AMI from one region to another with encryption"""

from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator

import core_logging as log

import core_framework as util

from core_framework.models import ActionSpec, DeploymentDetails

import core_helper.aws as aws

from core_execute.actionlib.action import BaseAction


class CopyImageActionParams(BaseModel):
    """
    Parameters for the CopyImageAction.

    :param account: The account to use for the action (required)
    :type account: str
    :param destination_image_name: The name of the destination image (required)
    :type destination_image_name: str
    :param image_name: The name of the source image (required)
    :type image_name: str
    :param kms_key_arn: The KMS key ARN to use for encryption (required)
    :type kms_key_arn: str
    :param region: The region to copy the image to (required)
    :type region: str
    :param tags: The tags to apply to the image (optional)
    :type tags: dict[str, str] | None
    """

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
    """
    Generate the action definition for CopyImageAction.

    This class provides default values and validation for CopyImageAction parameters.

    :param values: Dictionary of action specification values
    :type values: dict[str, Any]
    :return: Validated action specification values
    :rtype: dict[str, Any]
    """

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """
        Validate the parameters for the CopyImageActionSpec.

        :param values: Input values for validation
        :type values: dict[str, Any]
        :return: Validated and potentially modified values
        :rtype: dict[str, Any]
        """
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
    """
    Copy AMI from one region to another with encryption.

    This action will copy an AMI from one region to another with KMS encryption.
    The action will wait for the copy to complete before returning.

    :param definition: The action specification containing configuration details
    :type definition: ActionSpec
    :param context: The Jinja2 rendering context containing all variables
    :type context: dict[str, Any]
    :param deployment_details: Client/portfolio/app/branch/build information
    :type deployment_details: DeploymentDetails

    .. rubric:: Parameters

    :Name: Enter a name to define this action instance
    :Kind: Use the value ``AWS::CopyImage`` (not AWS::KMS::CopyImage)
    :Params.Account: The account where the destination region is located
    :Params.Region: The region to copy the image to
    :Params.ImageName: The name of the source image (required)
    :Params.DestinationImageName: The name for the copied image (required)
    :Params.KmsKeyArn: The KMS Key ARN to use for encryption (required)
    :Params.Tags: Optional tags to apply to the copied image

    .. rubric:: ActionSpec Example

    .. code-block:: yaml

        - Name: action-aws-copyimage-name
          Kind: "AWS::CopyImage"
          Params:
            Account: "123456789012"
            Region: "ap-southeast-1"
            ImageName: "My-Image-Name"
            DestinationImageName: "My-Encrypted-Image-Copy"
            KmsKeyArn: "arn:aws:kms:ap-southeast-1:123456789012:key/your-kms-key-id"
            Tags:
              Environment: "production"
              Owner: "ops-team"
          Scope: "build"

    .. note::
        The action will automatically add a "DeliveredBy" tag if deployment_details.delivered_by is available.

    .. warning::
        The source image must exist in the current region before copying.
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
        """
        Execute the image copy operation.

        This method finds the source image by name and copies it to the destination
        with KMS encryption enabled.

        :raises: Sets action to failed if source image is not found or EC2 operations fail
        """
        log.trace("Executing CopyImageAction")

        # Validate required parameters
        if not self.params.image_name:
            self.set_failed("ImageName parameter is required")
            return

        if not self.params.destination_image_name:
            self.set_failed("DestinationImageName parameter is required")
            return

        # Obtain an EC2 client
        try:
            ec2_client = aws.ec2_client(
                region=self.params.region,
                role=util.get_provisioning_role_arn(self.params.account),
            )
        except Exception as e:
            log.error("Failed to create EC2 client: {}", e)
            self.set_failed(f"Failed to create EC2 client: {e}")
            return

        # Find image (provides image id and snapshot ids)
        log.debug("Finding image with name '{}'", self.params.image_name)

        try:
            response = ec2_client.describe_images(Filters=[{"Name": "name", "Values": [self.params.image_name]}])
        except Exception as e:
            log.error("Failed to describe images: {}", e)
            self.set_failed(f"Failed to describe images: {e}")
            return

        if len(response["Images"]) == 0:
            self.set_failed(f"Could not find image with name '{self.params.image_name}'")
            log.error("Could not find image with name '{}'", self.params.image_name)
            return

        if len(response["Images"]) > 1:
            log.warning("Multiple images found with name '{}', using the first one", self.params.image_name)

        source_image = response["Images"][0]
        source_image_id = source_image["ImageId"]

        # Set state outputs for source image information
        self.set_state("SourceImageId", source_image_id)
        self.set_state("SourceImageName", self.params.image_name)
        self.set_state("SourceRegion", self.params.region)
        self.set_state("SourceAccount", self.params.account)

        # Set outputs for source image information
        self.set_output("SourceImageId", source_image_id)
        self.set_output("SourceImageName", self.params.image_name)

        log.debug("Found image '{}' with name '{}'", source_image_id, self.params.image_name)

        # Encrypt AMI by copying source AMI with encryption option
        self.set_running("Copying and encrypting image")

        try:
            response = ec2_client.copy_image(
                Encrypted=True,
                KmsKeyId=self.params.kms_key_arn,
                Name=self.params.destination_image_name,
                SourceImageId=source_image_id,
                SourceRegion=self.params.region,
            )
        except Exception as e:
            log.error("Failed to copy image '{}': {}", source_image_id, e)
            self.set_failed(f"Failed to copy image '{source_image_id}': {e}")
            return

        new_image_id = response["ImageId"]

        # Set state outputs for destination image information
        self.set_state("ImageId", new_image_id)
        self.set_state("DestinationImageId", new_image_id)
        self.set_state("DestinationImageName", self.params.destination_image_name)
        self.set_state("DestinationRegion", self.params.region)
        self.set_state("DestinationAccount", self.params.account)
        self.set_state("KmsKeyArn", self.params.kms_key_arn)
        self.set_state("CopyStarted", True)

        # Set outputs for destination image information (for other actions to reference)
        self.set_output("ImageId", new_image_id)
        self.set_output("DestinationImageId", new_image_id)
        self.set_output("DestinationImageName", self.params.destination_image_name)
        self.set_output("KmsKeyArn", self.params.kms_key_arn)

        log.debug("Started copy operation, new image ID: '{}'", new_image_id)
        log.trace("CopyImageAction completed")

    def _check(self):
        """
        Check the status of the image copy operation.

        This method waits for the image copy to complete and applies tags
        to both the image and its snapshots when available.

        :raises: Sets action to failed if image is not found, in error state, or EC2 operations fail
        """
        log.trace("Checking CopyImageAction")

        # Obtain an EC2 client
        try:
            ec2_client = aws.ec2_client(
                region=self.params.region,
                role=util.get_provisioning_role_arn(self.params.account),
            )
        except Exception as e:
            log.error("Failed to create EC2 client: {}", e)
            self.set_failed(f"Failed to create EC2 client: {e}")
            return

        # Wait for image creation to complete / fail
        image_id = self.get_state("ImageId")
        if image_id is None:
            log.error("Internal error - state variable ImageId should have been set during action execution")
            self.set_failed("No image previously created - cannot continue")
            return

        log.debug("Checking availability of copied image '{}'", image_id)

        try:
            describe_images_response = ec2_client.describe_images(ImageIds=[image_id])
        except Exception as e:
            log.error("Failed to describe image '{}': {}", image_id, e)
            self.set_failed(f"Failed to describe image '{image_id}': {e}")
            return

        if len(describe_images_response["Images"]) == 0:
            self.set_failed(f"No images found with id '{image_id}'")
            log.error("No images found with id '{}'", image_id)
            return

        image_info = describe_images_response["Images"][0]
        state = image_info["State"]

        # Update state with current image information
        self.set_state("ImageState", state)
        self.set_state("LastChecked", util.get_timestamp_str())  # Assuming this utility exists

        if state == "available":
            self.set_running(f"Tagging image '{image_id}'")

            # Extract and store image details now that it's available
            self.set_state("ImageSize", image_info.get("Size", 0))
            self.set_state("ImageArchitecture", image_info.get("Architecture", ""))
            self.set_state("ImagePlatform", image_info.get("Platform", ""))
            self.set_state("ImageDescription", image_info.get("Description", ""))
            self.set_state("ImageCreationDate", image_info.get("CreationDate", ""))

            # Set outputs for the completed image
            self.set_output("ImageState", state)
            self.set_output("ImageSize", image_info.get("Size", 0))
            self.set_output("ImageArchitecture", image_info.get("Architecture", ""))

            # Tag the image
            try:
                ec2_client.create_tags(Resources=[image_id], Tags=self.tags)
                log.debug("Successfully tagged image '{}'", image_id)
                self.set_state("ImageTagged", True)
            except Exception as e:
                log.warning("Failed to tag image '{}': {}", image_id, e)
                self.set_state("ImageTagged", False)
                # Don't fail the action for tagging errors, just warn

            # Tag the snapshots
            image_snapshots = self.__get_image_snapshots(describe_images_response)
            if len(image_snapshots) > 0:
                self.set_running(f"Tagging image snapshots: '{', '.join(image_snapshots)}'")

                # Store snapshot information
                self.set_state("SnapshotIds", image_snapshots)
                self.set_output("SnapshotIds", image_snapshots)

                try:
                    ec2_client.create_tags(Resources=image_snapshots, Tags=self.tags)
                    log.debug("Successfully tagged snapshots: {}", image_snapshots)
                    self.set_state("SnapshotsTagged", True)
                except Exception as e:
                    log.warning("Failed to tag snapshots {}: {}", image_snapshots, e)
                    self.set_state("SnapshotsTagged", False)
                    # Don't fail the action for tagging errors, just warn
            else:
                self.set_state("SnapshotIds", [])
                self.set_state("SnapshotsTagged", True)  # No snapshots to tag

            # Set final completion state
            self.set_state("CopyCompleted", True)
            self.set_state("CompletionTime", util.get_timestamp_str())
            self.set_output("CopyCompleted", True)

            self.set_complete(f"Image copy completed successfully. Image is '{state}'")

        elif state in ["pending", "transient"]:
            self.set_state("CopyCompleted", False)
            self.set_running(f"Image copy in progress. Image is '{state}'")
        elif state in ["failed", "error"]:
            self.set_state("CopyCompleted", False)
            self.set_state("CopyFailed", True)
            self.set_state("FailureReason", f"Image is in state '{state}'")
            self.set_failed(f"Image copy failed. Image is in state '{state}'")
        else:
            log.warning("Unknown image state: '{}'", state)
            self.set_state("CopyCompleted", False)
            self.set_running(f"Image is in unknown state '{state}'")

        log.trace("CopyImageAction check completed")

    def _unexecute(self):
        """
        Rollback the image copy operation.

        .. note::
            Currently not implemented. The copied image will remain.
        """

    def _cancel(self):
        """
        Cancel the image copy operation.

        .. note::
            Currently not implemented. Running copy operations cannot be cancelled.
        """

    def _resolve(self):
        """
        Resolve template variables in action parameters.

        This method renders Jinja2 templates in the action parameters using the current context.
        """
        log.trace("Resolving CopyImageAction")

        self.params.account = self.renderer.render_string(self.params.account, self.context)
        self.params.destination_image_name = self.renderer.render_string(self.params.destination_image_name, self.context)
        self.params.image_name = self.renderer.render_string(self.params.image_name, self.context)
        self.params.kms_key_arn = self.renderer.render_string(self.params.kms_key_arn, self.context)
        self.params.region = self.renderer.render_string(self.params.region, self.context)

        log.trace("CopyImageAction resolved")

    def __get_image_snapshots(self, describe_images_response: dict) -> list[str]:
        """
        Extract snapshot IDs from the describe_images response.

        :param describe_images_response: Response from EC2 describe_images call
        :type describe_images_response: dict
        :return: List of snapshot IDs associated with the image
        :rtype: list[str]

        .. note::
            Not all images have snapshots (e.g., instance store-backed AMIs).
            This method safely handles missing or malformed BlockDeviceMappings.
        """
        snapshots = []

        try:
            images = describe_images_response.get("Images", [])
            if not images:
                log.trace("No images found in describe_images response")
                return snapshots

            image = images[0]
            block_device_mappings = image.get("BlockDeviceMappings", [])

            if not block_device_mappings:
                log.trace("No BlockDeviceMappings found for image '{}'", image.get("ImageId", "unknown"))
                return snapshots

            for mapping in block_device_mappings:
                # Check if this is an EBS-backed device
                ebs_info = mapping.get("Ebs")
                if ebs_info and isinstance(ebs_info, dict):
                    snapshot_id = ebs_info.get("SnapshotId")
                    if snapshot_id:
                        snapshots.append(snapshot_id)
                        log.trace("Found snapshot '{}' for device '{}'", snapshot_id, mapping.get("DeviceName", "unknown"))
                    else:
                        log.trace("EBS device '{}' has no SnapshotId", mapping.get("DeviceName", "unknown"))
                else:
                    # This might be an instance store device
                    log.trace("Non-EBS device found: '{}'", mapping.get("DeviceName", "unknown"))

        except (KeyError, IndexError, TypeError) as e:
            log.warning("Error extracting snapshot IDs from describe_images response: {}", e)
            log.trace("Response structure: {}", describe_images_response)

        log.debug("Found {} snapshots for image: {}", len(snapshots), snapshots)
        return snapshots
