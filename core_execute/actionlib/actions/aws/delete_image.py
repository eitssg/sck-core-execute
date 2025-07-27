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
    """
    Parameters for the DeleteImageAction.

    :param account: The account to use for the action (required)
    :type account: str
    :param region: The region where the image is located (required)
    :type region: str
    :param image_name: The name of the image to delete (required)
    :type image_name: str
    """

    model_config = ConfigDict(populate_by_name=True, validate_assignment=True)

    account: str = Field(
        ...,
        alias="Account",
        description="The account to use for the action (required)",
    )
    region: str = Field(
        ...,
        alias="Region",
        description="The region where the image is located (required)",
    )
    image_name: str = Field(
        ...,
        alias="ImageName",
        description="The name of the image to delete (required)",
    )


class DeleteImageActionSpec(ActionSpec):
    """
    Generate the action definition for DeleteImageAction.

    This class provides default values and validation for DeleteImageAction parameters.

    :param values: Dictionary of action specification values
    :type values: dict[str, Any]
    :return: Validated action specification values
    :rtype: dict[str, Any]
    """

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """
        Validate the parameters for the DeleteImageActionSpec.

        :param values: Input values for validation
        :type values: dict[str, Any]
        :return: Validated and potentially modified values
        :rtype: dict[str, Any]
        """
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
    """
    Delete an AMI image and its associated snapshots.

    This action will delete an AMI image and its associated EBS snapshots.
    The action handles both existing and non-existing images gracefully.

    :param definition: The action specification containing configuration details
    :type definition: ActionSpec
    :param context: The Jinja2 rendering context containing all variables
    :type context: dict[str, Any]
    :param deployment_details: Client/portfolio/app/branch/build information
    :type deployment_details: DeploymentDetails

    .. rubric:: Parameters

    :Name: Enter a name to define this action instance
    :Kind: Use the value ``AWS::DeleteImage``
    :Params.Account: The account where the image is located (required)
    :Params.Region: The region where the image is located (required)
    :Params.ImageName: The name of the image to delete (required)

    .. rubric:: ActionSpec Example

    .. code-block:: yaml

        - Name: action-aws-deleteimage-name
          Kind: "AWS::DeleteImage"
          Params:
            Account: "154798051514"
            Region: "ap-southeast-1"
            ImageName: "my-image-name"
          Scope: "build"

    .. note::
        The action deletes both the AMI and all associated EBS snapshots.

    .. warning::
        Image deletion is irreversible and will delete all associated snapshots.
    """

    def __init__(
        self,
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        # Validate the action parameters
        self.params = DeleteImageActionParams(**definition.params)

    def _resolve(self):
        """
        Resolve template variables in action parameters.

        This method renders Jinja2 templates in the action parameters using the current context.
        """
        log.trace("Resolving DeleteImageAction")

        self.params.account = self.renderer.render_string(
            self.params.account, self.context
        )
        self.params.region = self.renderer.render_string(
            self.params.region, self.context
        )
        self.params.image_name = self.renderer.render_string(
            self.params.image_name, self.context
        )

        log.trace("DeleteImageAction resolved")

    def _execute(self):
        """
        Execute the AMI image deletion operation.

        This method deletes the specified AMI image and its associated snapshots,
        setting appropriate state outputs for tracking.

        :raises: Sets action to failed if image name is missing or EC2 operation fails
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
            response = ec2_client.describe_images(
                Filters=[{"Name": "name", "Values": [self.params.image_name]}]
            )

            if len(response["Images"]) == 0:
                log.warning("Image '{}' does not exist", self.params.image_name)
                self.set_state("ImageExists", False)
                self.set_state("DeletionCompleted", True)
                self.set_state("CompletionTime", util.get_current_timestamp())
                self.set_state("DeletionResult", "NOT_FOUND")

                self.set_output("DeletionCompleted", True)
                self.set_output("DeletionResult", "NOT_FOUND")

                self.set_complete(
                    f"Image '{self.params.image_name}' does not exist, may have been previously deleted"
                )
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
            self.set_failed(
                f"Failed to describe image '{self.params.image_name}': {e.response['Error']['Message']}"
            )
            return
        except Exception as e:
            log.error(
                "Unexpected error describing image '{}': {}", self.params.image_name, e
            )
            self.set_failed(
                f"Unexpected error describing image '{self.params.image_name}': {e}"
            )
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

                if (
                    error_code == "InvalidAMIID.Unavailable"
                    or error_code == "InvalidAMIID.NotFound"
                ):
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
                    self.set_state(
                        "DeregistrationFailureReason", f"{error_code}: {error_message}"
                    )
                    self.set_failed(
                        f"Failed to deregister image '{image_id}': {error_message}"
                    )
                    return

            except Exception as e:
                log.error("Unexpected error deregistering image '{}': {}", image_id, e)
                self.set_state("ImageDeregistrationFailed", True)
                self.set_state("DeregistrationFailureReason", str(e))
                self.set_failed(
                    f"Unexpected error deregistering image '{image_id}': {e}"
                )
                return

            # Delete image snapshots
            if snapshot_ids:
                self.set_running(
                    f"Deleting {len(snapshot_ids)} snapshots for image '{image_id}'"
                )

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
                            deleted_snapshots.append(
                                snapshot_id
                            )  # Treat as successfully deleted
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
                        failed_snapshots.append(
                            {"SnapshotId": snapshot_id, "Error": str(e)}
                        )

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
            self.set_output(
                "DeletedSnapshotCount", len(deleted_snapshots) if snapshot_ids else 0
            )

            self.set_complete(
                f"Successfully deleted image '{self.params.image_name}' (ID: {image_id}) and {len(deleted_snapshots) if snapshot_ids else 0} snapshots"
            )

        log.trace("DeleteImageAction execution completed")

    def _check(self):
        """
        Check the status of the image deletion operation.

        .. note::
            AMI deletion is synchronous, so this method should not be called.
        """
        log.trace("DeleteImageAction check")

        # AMI deletion is synchronous, so this shouldn't be called
        self.set_failed(
            "Internal error - _check() should not have been called for AMI deletion"
        )

        log.trace("DeleteImageAction check completed")

    def _unexecute(self):
        """
        Rollback the AMI image deletion operation.

        .. note::
            AMI deletion cannot be undone. This method is a no-op.
        """
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
        """
        Cancel the AMI image deletion operation.

        .. note::
            AMI deletion is synchronous and cannot be cancelled once started.
        """
        log.trace("Cancelling DeleteImageAction")

        # AMI deletion is synchronous and cannot be cancelled
        self.set_complete("AMI deletion cannot be cancelled")

        log.trace("DeleteImageAction cancellation completed")
