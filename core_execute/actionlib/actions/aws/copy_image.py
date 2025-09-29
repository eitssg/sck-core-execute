"""Copy an AMI from one region to another with encryption.

Finds the source AMI by name, copies it with KMS encryption, then waits
until the new AMI is available to tag the AMI and its snapshots.
"""

from typing import Any
from pydantic import Field, model_validator

import core_logging as log

import core_framework as util

from core_framework.models import ActionResource, ActionSpec, DeploymentDetails

import core_helper.aws as aws

from core_execute.actionlib.action import BaseAction


class CopyImageActionSpec(ActionSpec):
    """Parameters for copying an AMI with KMS encryption.

    Attributes:
      account: AWS account ID to use for role assumption.
      region: Destination AWS region for the copied AMI.
      image_name: Name of the source AMI to copy.
      destination_image_name: Name for the new (copied) AMI.
      kms_key_arn: KMS key ARN to encrypt the copied AMI.
      tags: Optional tags to apply to the copied AMI and its snapshots.
    """

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
    tags: dict[str, str] | None = Field(
        default_factory=dict,
        alias="Tags",
        description="The tags to apply to the image (optional)",
    )


class CopyImageActionResource(ActionResource):
    """Resource model for CopyImageAction (normalizes kind/spec)."""

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Normalize incoming values and enforce canonical kind/spec."""
        if not isinstance(values, dict):
            return values

        values.pop("kind", None)
        values.pop("Kind", None)
        values["kind"] = "AWS::CopyImage"

        spec = values.pop("spec", None) or values.pop("Spec", None)
        if isinstance(spec, dict):
            values["spec"] = spec
        elif isinstance(spec, CopyImageActionSpec):
            values["spec"] = spec.model_dump()

        return values


class CopyImageAction(BaseAction[CopyImageActionSpec]):
    """Copy an AMI to another region with KMS encryption and apply tags.

    - _execute: finds the source AMI by name and starts the copy
    - _check: waits for the copied AMI to be available and tags AMI/snapshots

    Args:
      definition: Action resource with metadata/spec.
      context: Rendering context for templates.
      deployment_details: Deployment metadata for this run.
    """

    def __init__(
        self,
        definition: ActionResource,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        # Validate the action parameters
        self.spec = CopyImageActionSpec.model_validate(definition.spec)

        tags = self.spec.tags or {}
        if deployment_details.delivered_by:
            tags["DeliveredBy"] = deployment_details.delivered_by or "unknown"

        self.tags = aws.transform_tag_hash(tags)

    def _execute(self):
        """Start the AMI copy and set initial state/outputs.

        Fails if required parameters are missing, the source AMI is not found,
        or EC2 client operations fail.
        """
        log.trace("Executing CopyImageAction")

        # Validate required parameters
        if not self.spec.image_name:
            self.set_failed("ImageName parameter is required")
            return

        if not self.spec.destination_image_name:
            self.set_failed("DestinationImageName parameter is required")
            return

        # Obtain an EC2 client
        try:
            ec2_client = aws.ec2_client(
                region=self.spec.region,
                role_arn=util.get_provisioning_role_arn(self.spec.account),
            )
        except Exception as e:
            log.error("Failed to create EC2 client: {}", e)
            self.set_failed(f"Failed to create EC2 client: {e}")
            return

        # Find image (provides image id and snapshot ids)
        log.debug("Finding image with name '{}'", self.spec.image_name)

        try:
            response = ec2_client.describe_images(Filters=[{"Name": "name", "Values": [self.spec.image_name]}])
        except Exception as e:
            log.error("Failed to describe images: {}", e)
            self.set_failed(f"Failed to describe images: {e}")
            return

        if len(response["Images"]) == 0:
            self.set_failed(f"Could not find image with name '{self.spec.image_name}'")
            log.error("Could not find image with name '{}'", self.spec.image_name)
            return

        if len(response["Images"]) > 1:
            log.warning(
                "Multiple images found with name '{}', using the first one",
                self.spec.image_name,
            )

        source_image = response["Images"][0]
        source_image_id = source_image["ImageId"]

        # Set state outputs for source image information
        self.set_state("SourceImageId", source_image_id)
        self.set_state("SourceImageName", self.spec.image_name)
        self.set_state("SourceRegion", self.spec.region)
        self.set_state("SourceAccount", self.spec.account)

        # Set outputs for source image information
        self.set_output("SourceImageId", source_image_id)
        self.set_output("SourceImageName", self.spec.image_name)

        log.debug("Found image '{}' with name '{}'", source_image_id, self.spec.image_name)

        # Encrypt AMI by copying source AMI with encryption option
        self.set_running("Copying and encrypting image")

        try:
            response = ec2_client.copy_image(
                Encrypted=True,
                KmsKeyId=self.spec.kms_key_arn,
                Name=self.spec.destination_image_name,
                SourceImageId=source_image_id,
                SourceRegion=self.spec.region,
            )
        except Exception as e:
            log.error("Failed to copy image '{}': {}", source_image_id, e)
            self.set_failed(f"Failed to copy image '{source_image_id}': {e}")
            return

        new_image_id = response["ImageId"]

        # Set state outputs for destination image information
        self.set_state("ImageId", new_image_id)
        self.set_state("DestinationImageId", new_image_id)
        self.set_state("DestinationImageName", self.spec.destination_image_name)
        self.set_state("DestinationRegion", self.spec.region)
        self.set_state("DestinationAccount", self.spec.account)
        self.set_state("KmsKeyArn", self.spec.kms_key_arn)
        self.set_state("CopyStarted", True)

        # Set outputs for destination image information (for other actions to reference)
        self.set_output("ImageId", new_image_id)
        self.set_output("DestinationImageId", new_image_id)
        self.set_output("DestinationImageName", self.spec.destination_image_name)
        self.set_output("KmsKeyArn", self.spec.kms_key_arn)

        log.debug("Started copy operation, new image ID: '{}'", new_image_id)
        log.trace("CopyImageAction completed")

    def _check(self):
        """Poll the copied AMI until available, then tag AMI and snapshots.

        Fails if the image cannot be found, is in an error state,
        or if EC2 operations fail.
        """
        log.trace("Checking CopyImageAction")

        # Obtain an EC2 client
        try:
            ec2_client = aws.ec2_client(
                region=self.spec.region,
                role_arn=util.get_provisioning_role_arn(self.spec.account),
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
        self.set_state("LastChecked", util.get_current_timestamp())  # Assuming this utility exists

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
            self.set_state("CompletionTime", util.get_current_timestamp())
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
        """No rollback; the copied image remains."""
        pass

    def _cancel(self):
        """No-op; running AMI copy operations cannot be cancelled."""
        pass

    def _resolve(self):
        """Render templates for account, region, names, and KMS key."""
        log.trace("Resolving CopyImageAction")

        self.spec.account = self.renderer.render_string(self.spec.account, self.context)
        self.spec.destination_image_name = self.renderer.render_string(self.spec.destination_image_name, self.context)
        self.spec.image_name = self.renderer.render_string(self.spec.image_name, self.context)
        self.spec.kms_key_arn = self.renderer.render_string(self.spec.kms_key_arn, self.context)
        self.spec.region = self.renderer.render_string(self.spec.region, self.context)

        log.trace("CopyImageAction resolved")

    def __get_image_snapshots(self, describe_images_response: dict) -> list[str]:
        """Return EBS snapshot IDs referenced by the described image.

        Args:
          describe_images_response: Response from EC2 describe_images.

        Returns:
          List of snapshot IDs associated with the image (may be empty).
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
                log.trace(
                    "No BlockDeviceMappings found for image '{}'",
                    image.get("ImageId", "unknown"),
                )
                return snapshots

            for mapping in block_device_mappings:
                # Check if this is an EBS-backed device
                ebs_info = mapping.get("Ebs")
                if ebs_info and isinstance(ebs_info, dict):
                    snapshot_id = ebs_info.get("SnapshotId")
                    if snapshot_id:
                        snapshots.append(snapshot_id)
                        log.trace(
                            "Found snapshot '{}' for device '{}'",
                            snapshot_id,
                            mapping.get("DeviceName", "unknown"),
                        )
                    else:
                        log.trace(
                            "EBS device '{}' has no SnapshotId",
                            mapping.get("DeviceName", "unknown"),
                        )
                else:
                    # This might be an instance store device
                    log.trace(
                        "Non-EBS device found: '{}'",
                        mapping.get("DeviceName", "unknown"),
                    )

        except (KeyError, IndexError, TypeError) as e:
            log.warning("Error extracting snapshot IDs from describe_images response: {}", e)
            log.trace("Response structure: {}", describe_images_response)

        log.debug("Found {} snapshots for image: {}", len(snapshots), snapshots)
        return snapshots

    @classmethod
    def generate_action_resource(cls, **kwargs) -> CopyImageActionResource:
        """Factory: create a typed CopyImageActionResource."""
        return CopyImageActionResource.model_validate(kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> CopyImageActionSpec:
        """Factory: create typed CopyImageActionSpec."""
        return CopyImageActionSpec.model_validate(kwargs)
