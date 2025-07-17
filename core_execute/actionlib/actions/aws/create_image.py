"""Create an Image of an EC2 instance"""

from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator

import core_logging as log

from core_framework.models import DeploymentDetails, ActionSpec

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


class CreateImageActionParams(BaseModel):
    """
    Parameters for the CreateImageAction.

    :param account: The account to use for the action (required)
    :type account: str
    :param image_name: The name of the image to create (required)
    :type image_name: str
    :param instance_id: The instance ID to create the image from (required)
    :type instance_id: str
    :param region: The region to create the image in (required)
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
    """
    Generate the action definition for CreateImageAction.

    This class provides default values and validation for CreateImageAction parameters.

    :param values: Dictionary of action specification values
    :type values: dict[str, Any]
    :return: Validated action specification values
    :rtype: dict[str, Any]
    """

    @model_validator(mode="before")
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """
        Validate the parameters for the CreateImageActionSpec.

        :param values: Input values for validation
        :type values: dict[str, Any]
        :return: Validated and potentially modified values
        :rtype: dict[str, Any]
        """
        if not (values.get("name") or values.get("Name")):
            values["name"] = "action-aws-createimage-name"
        if not (values.get("kind") or values.get("Kind")):
            values["kind"] = "AWS::CreateImage"
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
    """
    Create an AMI Image from an EC2 Instance.

    This action will create an AMI for an EC2 instance and wait for the operation to complete.
    The action will apply tags to both the image and associated snapshots when available.

    :param definition: The action specification containing configuration details
    :type definition: ActionSpec
    :param context: The Jinja2 rendering context containing all variables
    :type context: dict[str, Any]
    :param deployment_details: Client/portfolio/app/branch/build information
    :type deployment_details: DeploymentDetails

    .. rubric:: Parameters

    :Name: Enter a name to define this action instance
    :Kind: Use the value ``AWS::CreateImage`` (not AWS::KMS::CreateImage)
    :Params.Account: The account where the EC2 instance is located
    :Params.Region: The region where the EC2 instance is located
    :Params.InstanceId: The instance ID to create an image from (required)
    :Params.ImageName: The name of the image to create (required)
    :Params.Tags: Optional tags to apply to the created image

    .. rubric:: ActionSpec Example

    .. code-block:: yaml

        - Name: action-aws-createimage-name
          Kind: "AWS::CreateImage"
          Params:
            Account: "123456789012"
            Region: "ap-southeast-1"
            InstanceId: "i-1234567890abcdef0"
            ImageName: "My-Image-Name"
            Tags:
              Environment: "production"
              Project: "my-project"
          Scope: "build"

    .. note::
        The action will automatically add a "DeliveredBy" tag if deployment_details.delivered_by is available.

    .. warning::
        The source instance must be in a running or stopped state for image creation to succeed.
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
        """
        Execute the image creation operation.

        This method creates an AMI from the specified EC2 instance and sets
        appropriate state outputs for tracking.

        :raises: Sets action to failed if instance ID is missing or EC2 operations fail
        """
        log.trace("Executing CreateImageAction")

        # Validate required parameters
        if not self.params.instance_id or self.params.instance_id == "":
            self.set_failed("InstanceId parameter is required")
            log.error("InstanceId parameter is required")
            return

        if not self.params.image_name or self.params.image_name == "":
            self.set_failed("ImageName parameter is required")
            log.error("ImageName parameter is required")
            return

        # Set initial state information
        self.set_state("SourceInstanceId", self.params.instance_id)
        self.set_state("ImageName", self.params.image_name)
        self.set_state("Region", self.params.region)
        self.set_state("Account", self.params.account)

        # Set outputs for other actions to reference
        self.set_output("SourceInstanceId", self.params.instance_id)
        self.set_output("ImageName", self.params.image_name)

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

        # Create an image
        self.set_running(f"Creating new image '{self.params.image_name}'")

        try:
            response = ec2_client.create_image(InstanceId=self.params.instance_id, Name=self.params.image_name)
        except Exception as e:
            log.error("Failed to create image from instance '{}': {}", self.params.instance_id, e)
            self.set_failed(f"Failed to create image from instance '{self.params.instance_id}': {e}")
            return

        image_id = response["ImageId"]

        # Set comprehensive state outputs
        self.set_state("ImageId", image_id)
        self.set_state("ImageCreationStarted", True)

        # Set outputs for other actions to reference
        self.set_output("ImageId", image_id)
        self.set_output("ImageCreationStarted", True)

        log.debug("Image creation started with ID: '{}'", image_id)
        log.trace("CreateImageAction execution completed")

    def _check(self):
        """
        Check the status of the image creation operation.

        This method waits for the image creation to complete and applies tags
        to both the image and its snapshots when available.

        :raises: Sets action to failed if image is not found, in error state, or EC2 operations fail
        """
        log.trace("Checking CreateImageAction")

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

        log.debug("Checking availability of image '{}'", image_id)

        try:
            describe_images_response = ec2_client.describe_images(ImageIds=[image_id])
        except Exception as e:
            log.error("Failed to describe image '{}': {}", image_id, e)
            self.set_failed(f"Failed to describe image '{image_id}': {e}")
            return

        if len(describe_images_response["Images"]) == 0:
            log.error("No images found with id '{}'", image_id)
            self.set_failed(f"Could not find created image '{image_id}'")
            return

        image_info = describe_images_response["Images"][0]
        state = image_info["State"]

        # Update state with current image information
        self.set_state("ImageState", state)
        self.set_state("LastChecked", util.get_timestamp_str())

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
            self.set_state("ImageCreationCompleted", True)
            self.set_state("CompletionTime", util.get_timestamp_str())
            self.set_output("ImageCreationCompleted", True)

            self.set_complete("Image creation completed successfully")

        elif state == "pending":
            self.set_state("ImageCreationCompleted", False)
            self.set_running(f"Image creation in progress. Image is '{state}'")
        elif state in ["failed", "error"]:
            self.set_state("ImageCreationCompleted", False)
            self.set_state("ImageCreationFailed", True)
            self.set_state("FailureReason", f"Image is in state '{state}'")
            self.set_failed(f"Image creation failed. Image is in state '{state}'")
        else:
            log.warning("Unknown image state: '{}'", state)
            self.set_state("ImageCreationCompleted", False)
            self.set_running(f"Image is in unknown state '{state}'")

        log.trace("CreateImageAction check completed")

    def _unexecute(self):
        """
        Rollback the image creation operation.

        This method deregisters the created AMI and deletes associated snapshots.

        .. note::
            This will permanently delete the created image and cannot be undone.
        """
        log.trace("Unexecuting CreateImageAction")

        image_id = self.get_state("ImageId")
        if not image_id:
            log.debug("No image ID found in state - nothing to rollback")
            self.set_complete("No image to rollback")
            return

        try:
            ec2_client = aws.ec2_client(
                region=self.params.region,
                role=util.get_provisioning_role_arn(self.params.account),
            )
        except Exception as e:
            log.error("Failed to create EC2 client for rollback: {}", e)
            self.set_failed(f"Failed to create EC2 client for rollback: {e}")
            return

        try:
            # Get snapshot IDs before deregistering the image
            snapshot_ids = self.get_state("SnapshotIds") or []

            # Deregister the AMI
            ec2_client.deregister_image(ImageId=image_id)
            log.debug("Deregistered image '{}'", image_id)

            # Delete associated snapshots
            for snapshot_id in snapshot_ids:
                try:
                    ec2_client.delete_snapshot(SnapshotId=snapshot_id)
                    log.debug("Deleted snapshot '{}'", snapshot_id)
                except Exception as e:
                    log.warning("Failed to delete snapshot '{}': {}", snapshot_id, e)

            self.set_state("ImageRolledBack", True)
            self.set_complete("Image rollback completed successfully")

        except Exception as e:
            log.error("Failed to rollback image '{}': {}", image_id, e)
            self.set_failed(f"Failed to rollback image '{image_id}': {e}")

    def _cancel(self):
        """
        Cancel the image creation operation.

        .. note::
            EC2 image creation cannot be cancelled once started. This method is a no-op.
        """
        log.trace("CreateImageAction cancel - image creation cannot be cancelled")
        self.set_complete("Image creation cannot be cancelled")

    def _resolve(self):
        """
        Resolve template variables in action parameters.

        This method renders Jinja2 templates in the action parameters using the current context.
        """
        log.trace("Resolving CreateImageAction")

        self.params.account = self.renderer.render_string(self.params.account, self.context)
        self.params.image_name = self.renderer.render_string(self.params.image_name, self.context)
        self.params.instance_id = self.renderer.render_string(self.params.instance_id, self.context)
        self.params.region = self.renderer.render_string(self.params.region, self.context)

        log.trace("CreateImageAction resolved")

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

        except (KeyError, IndexError, TypeError) as e:
            log.warning("Error extracting snapshot IDs from describe_images response: {}", e)
            log.trace("Response structure: {}", describe_images_response)

        log.debug("Found {} snapshots for image: {}", len(snapshots), snapshots)
        return snapshots
