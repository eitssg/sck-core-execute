"""Delete an AMI and its associated snapshots.

Finds the image by name, deregisters it, and deletes related EBS snapshots.
Records progress and results in action state and outputs.
"""

from typing import Any
from pydantic import Field, model_validator

import core_logging as log

from core_framework.models import DeploymentDetails, ActionSpec, ActionResource

from botocore.exceptions import ClientError

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


class DeleteImageActionSpec(ActionSpec):
    """Parameters for deleting an AMI and its snapshots.

    Attributes:
      account: AWS account ID for the action.
      region: AWS region where the image resides.
      image_name: Name of the image (AMI) to delete.
    """

    image_name: str = Field(
        ...,
        alias="ImageName",
        description="The name of the image to delete (required)",
    )


class DeleteImageActionResource(ActionResource):
    """Resource model for DeleteImageAction (normalizes kind/spec)."""

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Normalize incoming values and enforce canonical kind/spec."""
        if not isinstance(values, dict):
            return values

        values.pop("kind", None)
        values.pop("Kind", None)
        values["kind"] = "AWS::DeleteImage"

        spec = values.pop("spec", None) or values.pop("Spec", None)
        if isinstance(spec, dict):
            values["spec"] = spec
        elif isinstance(spec, DeleteImageActionSpec):
            values["spec"] = spec.model_dump()

        return values


class DeleteImageAction(BaseAction):
    """Delete an AMI and its associated EBS snapshots.

    - Treats missing images as success with a message
    - Deregisters the AMI and deletes referenced snapshots
    - Stores details, counts, and results in state/outputs
    """

    def __init__(
        self,
        definition: ActionResource,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
        parent_action_name: str | None = None,
    ):
        """Initialize the action and validate parameters.

        Args:
          definition: Action resource with metadata/spec.
          context: Rendering context for templates.
          deployment_details: Deployment metadata for this run.
          parent_action_name: Optional parent action name.
        """
        super().__init__(definition, context, deployment_details, parent_action_name)

        # Validate the action parameters
        self.params = DeleteImageActionSpec(**definition.spec)

    def _resolve(self):
        """Render template variables in account, region, and image_name."""
        log.trace("Resolving DeleteImageAction")

        self.params.account = self.renderer.render_string(self.params.account, self.context)
        self.params.region = self.renderer.render_string(self.params.region, self.context)
        self.params.image_name = self.renderer.render_string(self.params.image_name, self.context)

        log.trace("DeleteImageAction resolved")

    def _execute(self):
        """Delete the AMI and referenced snapshots; set state/outputs.

        Notes:
          - Fails if ImageName is missing or EC2 calls fail unexpectedly.
          - Snapshot deletion failures are recorded but do not fail the action.
        """
        log.trace("Executing DeleteImageAction")

        # Validate required parameters
        if not self.params.image_name or self.params.image_name == "":
            self.set_failed("ImageName parameter is required")
            log.error("ImageName parameter is required")
            return

        # Set initial state information
        self.set_state("ImageName", self.params.image_name)
        self.set_state("Region", self.params.region)
        self.set_state("Account", self.params.account)
        self.set_state("DeletionStarted", True)
        self.set_state("StartTime", util.get_current_timestamp())

        # Set outputs for other actions to reference
        self.set_output("ImageName", self.params.image_name)
        self.set_output("Region", self.params.region)
        self.set_output("DeletionStarted", True)

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
        image_id = None
        snapshot_ids = []
        image_exists = False

        try:
            log.debug("Finding image with name '{}'", self.params.image_name)
            response = ec2_client.describe_images(Filters=[{"Name": "name", "Values": [self.params.image_name]}])

            if len(response["Images"]) == 0:
                log.warning("Image '{}' does not exist", self.params.image_name)
                self.set_state("ImageExists", False)
                self.set_state("DeletionCompleted", True)
                self.set_state("CompletionTime", util.get_current_timestamp())
                self.set_state("DeletionResult", "NOT_FOUND")

                self.set_output("DeletionCompleted", True)
                self.set_output("DeletionResult", "NOT_FOUND")

                self.set_complete(f"Image '{self.params.image_name}' does not exist, may have been previously deleted")
                return

            image_info = response["Images"][0]
            image_id = image_info["ImageId"]
            image_exists = True

            # Store image information before deletion
            self.set_state("ImageId", image_id)
            self.set_state("ImageExists", True)
            self.set_state("ImageDescription", image_info.get("Description", ""))
            self.set_state("ImageArchitecture", image_info.get("Architecture", ""))
            self.set_state("ImageState", image_info.get("State", ""))
            self.set_state("ImageCreationDate", image_info.get("CreationDate", ""))

            log.debug("Found image '{}' with id '{}'", self.params.image_name, image_id)

            # Extract snapshot ids from describe_images response
            for block_device_mapping in image_info["BlockDeviceMappings"]:
                if "Ebs" not in block_device_mapping:
                    continue
                snapshot_id = block_device_mapping["Ebs"]["SnapshotId"]
                snapshot_ids.append(snapshot_id)

            self.set_state("SnapshotIds", snapshot_ids)
            self.set_state("SnapshotCount", len(snapshot_ids))

            log.debug(
                "Image '{}' has {} snapshots: {}",
                image_id,
                len(snapshot_ids),
                snapshot_ids,
            )

        except ClientError as e:
            log.error(
                "Error describing image '{}': {}",
                self.params.image_name,
                e.response["Error"]["Message"],
            )
            self.set_failed(f"Failed to describe image '{self.params.image_name}': {e.response['Error']['Message']}")
            return
        except Exception as e:
            log.error("Unexpected error describing image '{}': {}", self.params.image_name, e)
            self.set_failed(f"Unexpected error describing image '{self.params.image_name}': {e}")
            return

        # Deregister image
        if image_exists and image_id:
            self.set_running(f"Deregistering image '{image_id}'")

            try:
                ec2_client.deregister_image(ImageId=image_id)
                self.set_state("ImageDeregistered", True)
                log.debug("Successfully deregistered image '{}'", image_id)

            except ClientError as e:
                error_code = e.response["Error"]["Code"]
                error_message = e.response["Error"]["Message"]

                if error_code == "InvalidAMIID.Unavailable" or error_code == "InvalidAMIID.NotFound":
                    log.warning(
                        "Image '{}' was not found during deregistration (may have been deleted concurrently): {}",
                        image_id,
                        error_message,
                    )
                    self.set_state("ImageDeregistered", True)
                    self.set_state("ImageNotFoundDuringDeregistration", True)
                else:
                    log.error(
                        "Error deregistering image '{}': {} - {}",
                        image_id,
                        error_code,
                        error_message,
                    )
                    self.set_state("ImageDeregistrationFailed", True)
                    self.set_state("DeregistrationFailureReason", f"{error_code}: {error_message}")
                    self.set_failed(f"Failed to deregister image '{image_id}': {error_message}")
                    return

            except Exception as e:
                log.error("Unexpected error deregistering image '{}': {}", image_id, e)
                self.set_state("ImageDeregistrationFailed", True)
                self.set_state("DeregistrationFailureReason", str(e))
                self.set_failed(f"Unexpected error deregistering image '{image_id}': {e}")
                return

            # Delete image snapshots
            if snapshot_ids:
                self.set_running(f"Deleting {len(snapshot_ids)} snapshots for image '{image_id}'")

                deleted_snapshots = []
                failed_snapshots = []

                for snapshot_id in snapshot_ids:
                    log.debug("Deleting snapshot '{}'", snapshot_id)

                    try:
                        ec2_client.delete_snapshot(SnapshotId=snapshot_id)
                        deleted_snapshots.append(snapshot_id)
                        log.debug("Successfully deleted snapshot '{}'", snapshot_id)

                    except ClientError as e:
                        error_code = e.response["Error"]["Code"]
                        error_message = e.response["Error"]["Message"]

                        if error_code == "InvalidSnapshot.NotFound":
                            log.warning(
                                "Snapshot '{}' was not found during deletion (may have been deleted concurrently): {}",
                                snapshot_id,
                                error_message,
                            )
                            deleted_snapshots.append(snapshot_id)  # Treat as successfully deleted
                        elif error_code == "InvalidSnapshot.InUse":
                            log.warning(
                                "Snapshot '{}' is in use and cannot be deleted: {}",
                                snapshot_id,
                                error_message,
                            )
                            failed_snapshots.append(
                                {
                                    "SnapshotId": snapshot_id,
                                    "Error": f"{error_code}: {error_message}",
                                }
                            )
                        else:
                            log.error(
                                "Error deleting snapshot '{}': {} - {}",
                                snapshot_id,
                                error_code,
                                error_message,
                            )
                            failed_snapshots.append(
                                {
                                    "SnapshotId": snapshot_id,
                                    "Error": f"{error_code}: {error_message}",
                                }
                            )

                    except Exception as e:
                        log.error(
                            "Unexpected error deleting snapshot '{}': {}",
                            snapshot_id,
                            e,
                        )
                        failed_snapshots.append({"SnapshotId": snapshot_id, "Error": str(e)})

                # Store snapshot deletion results
                self.set_state("DeletedSnapshots", deleted_snapshots)
                self.set_state("FailedSnapshots", failed_snapshots)
                self.set_state("DeletedSnapshotCount", len(deleted_snapshots))
                self.set_state("FailedSnapshotCount", len(failed_snapshots))

                if failed_snapshots:
                    log.warning(
                        "Failed to delete {} out of {} snapshots for image '{}'",
                        len(failed_snapshots),
                        len(snapshot_ids),
                        image_id,
                    )
                    # Don't fail the action for snapshot deletion failures, just log them
                log.debug(
                    "Deleted {} out of {} snapshots for image '{}'",
                    len(deleted_snapshots),
                    len(snapshot_ids),
                    image_id,
                )

            # Set completion state
            self.set_state("DeletionCompleted", True)
            self.set_state("CompletionTime", util.get_current_timestamp())
            self.set_state("DeletionResult", "SUCCESS")

            self.set_output("DeletionCompleted", True)
            self.set_output("DeletionResult", "SUCCESS")
            self.set_output("ImageId", image_id)
            self.set_output("DeletedSnapshotCount", len(deleted_snapshots) if snapshot_ids else 0)

            self.set_complete(
                f"Successfully deleted image '{self.params.image_name}' (ID: {image_id}) and {len(deleted_snapshots) if snapshot_ids else 0} snapshots"
            )

        log.trace("DeleteImageAction execution completed")

    def _check(self):
        """Not applicable; AMI deletion is synchronous."""
        log.trace("DeleteImageAction check")

        # AMI deletion is synchronous, so this shouldn't be called
        self.set_failed("Internal error - _check() should not have been called for AMI deletion")

        log.trace("DeleteImageAction check completed")

    def _unexecute(self):
        """No rollback; AMI deletion cannot be undone."""
        log.trace("Unexecuting DeleteImageAction")

        # AMI deletion cannot be undone
        image_name = self.params.image_name
        log.warning(
            "AMI deletion cannot be rolled back - image '{}' remains deleted",
            image_name,
        )

        self.set_state("RollbackAttempted", True)
        self.set_state("RollbackResult", "NOT_POSSIBLE")

        self.set_complete("AMI deletion cannot be rolled back")

        log.trace("DeleteImageAction unexecution completed")

    def _cancel(self):
        """No-op; AMI deletion is synchronous and cannot be cancelled."""
        log.trace("Cancelling DeleteImageAction")

        # AMI deletion is synchronous and cannot be cancelled
        self.set_complete("AMI deletion cannot be cancelled")

        log.trace("DeleteImageAction cancellation completed")

    @classmethod
    def generate_action_resource(cls, **kwargs) -> DeleteImageActionResource:
        """Factory: create a typed DeleteImageActionResource."""
        return DeleteImageActionResource(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> DeleteImageActionSpec:
        """Factory: create typed DeleteImageActionSpec."""
        return DeleteImageActionSpec(**kwargs)
