"""Delete ENIs attached to a security group.

Detaches in-use ENIs (except AWS hyperplane-managed) and deletes available ENIs.
"""

from typing import Any
from pydantic import Field, model_validator
from botocore.exceptions import ClientError

import core_logging as log

from core_framework.models import DeploymentDetails, ActionResource, ActionSpec

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


# If this account is hyperplane enabled, amazon manages the ENI attachments for you.
ENI_OWNER_HYPERPLANE = "amazon-aws"


class DeleteSecurityGroupEnisActionSpec(ActionSpec):
    """Parameters for deleting ENIs attached to a security group.

    Attributes:
      account: AWS account ID used for the action.
      region: AWS region of the security group.
      security_group_id: ID of the security group whose ENIs will be removed.
    """

    security_group_id: str = Field(
        ...,
        alias="SecurityGroupId",
        description="The ID of the security group to delete ENIs from (required)",
    )


class DeleteSecurityGroupEnisActionResource(ActionResource):
    """Resource model for DeleteSecurityGroupEnis.

    Normalizes inputs and forces kind to 'AWS::DeleteSecurityGroupEnis'.
    """

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Normalize incoming values and enforce canonical kind/spec."""
        if not isinstance(values, dict):
            return values

        values.pop("kind", None)
        values.pop("Kind", None)
        values["kind"] = "AWS::DeleteSecurityGroupEnis"

        spec = values.pop("spec", None) or values.pop("Spec", None)
        if isinstance(spec, dict):
            values["spec"] = spec
        elif isinstance(spec, DeleteSecurityGroupEnisActionSpec):
            values["spec"] = spec.model_dump()

        return values


class DeleteSecurityGroupEnisAction(BaseAction):
    """Detach and delete ENIs attached to a security group.

    - Detaches in-use ENIs (non-hyperplane) and deletes available ENIs
    - Treats missing security group as success
    - Tracks progress across _execute and _check
    """

    def __init__(
        self,
        definition: ActionResource,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
        parent_action_name: str | None = None,
    ):
        """Initialize action and validate parameters."""
        super().__init__(definition, context, deployment_details, parent_action_name)

        # Validate and set the parameters
        self.params = DeleteSecurityGroupEnisActionSpec(**definition.spec)

    def _resolve(self):
        """Render template variables in account, region, and security_group_id."""
        log.trace("Resolving DeleteSecurityGroupEnisAction")

        self.params.account = self.renderer.render_string(self.params.account, self.context)
        self.params.region = self.renderer.render_string(self.params.region, self.context)
        self.params.security_group_id = self.renderer.render_string(self.params.security_group_id, self.context)

        log.trace("DeleteSecurityGroupEnisAction resolved")

    def _execute(self):
        """Start or continue ENI deletion and set initial state."""
        log.trace("Executing DeleteSecurityGroupEnisAction")

        # Validate required parameters
        if not self.params.security_group_id or self.params.security_group_id == "":
            self.set_failed("SecurityGroupId parameter is required")
            log.error("SecurityGroupId parameter is required")
            return

        # Set initial state information
        self.set_state("SecurityGroupId", self.params.security_group_id)
        self.set_state("Region", self.params.region)
        self.set_state("Account", self.params.account)
        self.set_state("DeletionStarted", True)
        self.set_state("StartTime", util.get_current_timestamp())

        # Set outputs for other actions to reference
        self.set_output("SecurityGroupId", self.params.security_group_id)
        self.set_output("Region", self.params.region)
        self.set_output("DeletionStarted", True)

        self.set_running(f"Deleting ENIs attached to security group '{self.params.security_group_id}'")
        self._detach_enis()

        log.trace("DeleteSecurityGroupEnisAction execution completed")

    def _check(self):
        """Continue ENI deletion in subsequent batches."""
        log.trace("Checking DeleteSecurityGroupEnisAction")

        self._detach_enis()

        log.trace("DeleteSecurityGroupEnisAction check completed")

    def _unexecute(self):
        """No rollback; ENI deletion is irreversible."""
        log.trace("Unexecuting DeleteSecurityGroupEnisAction")

        # ENI deletion cannot be undone
        log.warning(
            "ENI deletion cannot be rolled back - ENIs for security group '{}' remain deleted",
            self.params.security_group_id,
        )

        self.set_state("RollbackAttempted", True)
        self.set_state("RollbackResult", "NOT_POSSIBLE")

        self.set_complete("ENI deletion cannot be rolled back")

        log.trace("DeleteSecurityGroupEnisAction unexecution completed")

    def _cancel(self):
        """No-op; deletion operations cannot be cancelled."""
        log.trace("Cancelling DeleteSecurityGroupEnisAction")

        # ENI operations cannot be cancelled once started
        self.set_complete("ENI deletion operations cannot be cancelled")

        log.trace("DeleteSecurityGroupEnisAction cancellation completed")

    def _detach_enis(self):
        """Find, detach, and delete ENIs for the target security group.

        - Skips hyperplane-managed ENIs
        - Detaches in-use ENIs then deletes when available
        - Updates state/outputs with progress and results
        """
        log.trace("Processing ENIs for security group '{}'", self.params.security_group_id)

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

        # Retrieve security group ENIs
        try:
            response = ec2_client.describe_network_interfaces(
                Filters=[{"Name": "group-id", "Values": [self.params.security_group_id]}]
            )
            network_interfaces = response["NetworkInterfaces"]

            log.debug(
                "Found {} ENIs attached to security group '{}'",
                len(network_interfaces),
                self.params.security_group_id,
            )

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            error_message = e.response["Error"]["Message"]

            if error_code == "InvalidGroup.NotFound":
                log.warning(
                    "Security group '{}' not found: {}",
                    self.params.security_group_id,
                    error_message,
                )
                self.set_state("SecurityGroupExists", False)
                self.set_state("DeletionCompleted", True)
                self.set_state("CompletionTime", util.get_current_timestamp())
                self.set_state("DeletionResult", "SECURITY_GROUP_NOT_FOUND")

                self.set_output("DeletionCompleted", True)
                self.set_output("DeletionResult", "SECURITY_GROUP_NOT_FOUND")

                self.set_complete(f"Security group '{self.params.security_group_id}' not found, no ENIs to delete")
                return
            else:
                log.error(
                    "Error describing network interfaces for security group '{}': {} - {}",
                    self.params.security_group_id,
                    error_code,
                    error_message,
                )
                self.set_failed(f"Failed to describe network interfaces: {error_message}")
                return

        except Exception as e:
            log.error("Unexpected error describing network interfaces: {}", e)
            self.set_failed(f"Unexpected error describing network interfaces: {e}")
            return

        # Get previous state for tracking across iterations
        previous_detached_enis = self.get_state("DetachedEnis", [])
        previous_deleted_enis = self.get_state("DeletedEnis", [])
        previous_skipped_enis = self.get_state("SkippedEnis", [])
        previous_failed_enis = self.get_state("FailedEnis", [])
        previous_in_use_enis = self.get_state("InUseEnis", [])

        # Initialize tracking lists with previous results
        detached_enis = list(previous_detached_enis)
        deleted_enis = list(previous_deleted_enis)
        skipped_enis = list(previous_skipped_enis)
        failed_enis = list(previous_failed_enis)
        in_use_enis = list(previous_in_use_enis)

        # Track total ENIs found (first time only)
        if not self.get_state("TotalEnisFound", None):
            total_enis = len(network_interfaces)
            self.set_state("TotalEnisFound", total_enis)

        self.set_state("SecurityGroupExists", True)

        if not network_interfaces:
            # No ENIs found - check if we had any in previous iterations
            if not (detached_enis or deleted_enis or skipped_enis):
                # Truly no ENIs ever found
                self.set_state("DeletionCompleted", True)
                self.set_state("CompletionTime", util.get_current_timestamp())
                self.set_state("DeletionResult", "SUCCESS")
                self.set_state("ProcessedEniCount", 0)
                self.set_state("DeletedEniCount", 0)
                self.set_state("DetachedEniCount", 0)

                self.set_output("DeletionCompleted", True)
                self.set_output("DeletionResult", "SUCCESS")
                self.set_output("ProcessedEniCount", 0)

                self.set_complete(f"No ENIs found attached to security group '{self.params.security_group_id}'")
                return
            else:
                # No more ENIs found - all previous ENIs have been processed
                log.debug("No more ENIs found - all previous ENIs have been processed")
                # Fall through to completion logic
                in_use_enis = []  # Clear the in_use list since no more ENIs exist

        # Process each current ENI
        for network_interface in network_interfaces:
            eni_id = network_interface["NetworkInterfaceId"]
            eni_status = network_interface["Status"]

            log.debug("Processing ENI '{}' with status '{}'", eni_id, eni_status)

            # Check if this ENI was already processed
            already_processed = any(
                eni_id == item.get("EniId") for item in (detached_enis + deleted_enis + skipped_enis + failed_enis)
            )

            if already_processed:
                log.debug("ENI '{}' already processed in previous iteration", eni_id)

                # If this ENI was previously detached and is now available, delete it
                if (
                    eni_status == "available"
                    and eni_id in in_use_enis
                    and any(item.get("EniId") == eni_id for item in detached_enis)
                ):

                    try:
                        log.debug(
                            "Deleting previously detached ENI '{}' which is now available",
                            eni_id,
                        )
                        ec2_client.delete_network_interface(NetworkInterfaceId=eni_id)

                        # Move from detached to deleted
                        deleted_enis.append({"EniId": eni_id, "Status": eni_status})
                        in_use_enis.remove(eni_id)  # Remove from in_use tracking

                        log.debug("Successfully deleted previously detached ENI '{}'", eni_id)

                    except ClientError as e:
                        error_code = e.response["Error"]["Code"]
                        error_message = e.response["Error"]["Message"]

                        log.error(
                            "Error deleting previously detached ENI '{}': {} - {}",
                            eni_id,
                            error_code,
                            error_message,
                        )
                        failed_enis.append({"EniId": eni_id, "Error": f"{error_code}: {error_message}"})
                        in_use_enis.remove(eni_id)  # Remove from in_use tracking even on failure

                    except Exception as e:
                        log.error(
                            "Unexpected error deleting previously detached ENI '{}': {}",
                            eni_id,
                            e,
                        )
                        failed_enis.append({"EniId": eni_id, "Error": str(e)})
                        in_use_enis.remove(eni_id)  # Remove from in_use tracking even on failure

                continue  # Skip to next ENI

            # Process new ENIs (first time encountering this ENI)
            try:
                if eni_status == "in-use":
                    # Check if this is a hyperplane-managed ENI
                    attachment = network_interface.get("Attachment", {})
                    instance_owner_id = attachment.get("InstanceOwnerId", "")

                    if instance_owner_id == ENI_OWNER_HYPERPLANE:
                        log.debug(
                            "Skipping hyperplane-managed ENI '{}' - AWS will handle detachment",
                            eni_id,
                        )
                        skipped_enis.append(
                            {
                                "EniId": eni_id,
                                "Reason": "Hyperplane-managed",
                                "Status": eni_status,
                            }
                        )
                    else:
                        # Detach 'in-use' ENIs that are not hyperplane-managed
                        attachment_id = attachment.get("AttachmentId")
                        if attachment_id:
                            log.debug(
                                "Detaching ENI '{}' from security group '{}'",
                                eni_id,
                                self.params.security_group_id,
                            )

                            ec2_client.detach_network_interface(
                                AttachmentId=attachment_id,
                                Force=True,
                            )

                            detached_enis.append(
                                {
                                    "EniId": eni_id,
                                    "AttachmentId": attachment_id,
                                    "InstanceOwnerId": instance_owner_id,
                                }
                            )
                            in_use_enis.append(eni_id)

                            log.debug("Successfully detached ENI '{}'", eni_id)
                        else:
                            log.warning("ENI '{}' is in-use but has no attachment ID", eni_id)
                            failed_enis.append(
                                {
                                    "EniId": eni_id,
                                    "Error": "No attachment ID found for in-use ENI",
                                }
                            )

                elif eni_status == "available":
                    # Delete 'available' ENIs
                    log.debug("Deleting available ENI '{}'", eni_id)

                    ec2_client.delete_network_interface(NetworkInterfaceId=eni_id)

                    deleted_enis.append({"EniId": eni_id, "Status": eni_status})

                    log.debug("Successfully deleted ENI '{}'", eni_id)

                else:
                    log.warning(
                        "ENI '{}' has unexpected status '{}', skipping",
                        eni_id,
                        eni_status,
                    )
                    skipped_enis.append(
                        {
                            "EniId": eni_id,
                            "Reason": f"Unexpected status: {eni_status}",
                            "Status": eni_status,
                        }
                    )

            except ClientError as e:
                error_code = e.response["Error"]["Code"]
                error_message = e.response["Error"]["Message"]

                log.error(
                    "Error processing ENI '{}': {} - {}",
                    eni_id,
                    error_code,
                    error_message,
                )
                failed_enis.append({"EniId": eni_id, "Error": f"{error_code}: {error_message}"})

            except Exception as e:
                log.error("Unexpected error processing ENI '{}': {}", eni_id, e)
                failed_enis.append({"EniId": eni_id, "Error": str(e)})

        # Store processing results in state
        self.set_state("DetachedEnis", detached_enis)
        self.set_state("DeletedEnis", deleted_enis)
        self.set_state("SkippedEnis", skipped_enis)
        self.set_state("FailedEnis", failed_enis)
        self.set_state("InUseEnis", in_use_enis)

        self.set_state("DetachedEniCount", len(detached_enis))
        self.set_state("DeletedEniCount", len(deleted_enis))
        self.set_state("SkippedEniCount", len(skipped_enis))
        self.set_state("FailedEniCount", len(failed_enis))
        self.set_state("InUseEniCount", len(in_use_enis))

        # Determine if operation is complete
        if in_use_enis:
            # Still have in-use ENIs that were detached, need to wait for them to become available
            log.debug(
                "Waiting for {} detached ENIs to become available: {}",
                len(in_use_enis),
                in_use_enis,
            )
            self.set_running(f"Waiting for {len(in_use_enis)} detached ENIs to become available for deletion")
            return

        # Operation complete
        total_enis = self.get_state("TotalEnisFound", 0)
        total_processed = len(detached_enis) + len(deleted_enis)

        if failed_enis:
            self.set_state("DeletionResult", "PARTIAL_SUCCESS")
            self.set_state("DeletionCompleted", True)
            self.set_state("CompletionTime", util.get_current_timestamp())

            self.set_output("DeletionCompleted", True)
            self.set_output("DeletionResult", "PARTIAL_SUCCESS")
            self.set_output("ProcessedEniCount", total_processed)
            self.set_output("FailedEniCount", len(failed_enis))

            log.warning(
                "Completed ENI deletion with {} failures out of {} total ENIs",
                len(failed_enis),
                total_enis,
            )
            self.set_complete(
                f"Processed {total_processed} ENIs with {len(failed_enis)} failures for security group '{self.params.security_group_id}'"
            )
        else:
            self.set_state("DeletionResult", "SUCCESS")
            self.set_state("DeletionCompleted", True)
            self.set_state("CompletionTime", util.get_current_timestamp())

            self.set_output("DeletionCompleted", True)
            self.set_output("DeletionResult", "SUCCESS")
            self.set_output("ProcessedEniCount", total_processed)

            self.set_complete(
                f"Successfully processed all {total_processed} ENIs for security group '{self.params.security_group_id}'"
            )

        log.trace(
            "ENI processing completed for security group '{}'",
            self.params.security_group_id,
        )

    @classmethod
    def generate_action_resource(cls, **kwargs) -> DeleteSecurityGroupEnisActionResource:
        """Factory: create a typed DeleteSecurityGroupEnisActionResource."""
        return DeleteSecurityGroupEnisActionResource(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> DeleteSecurityGroupEnisActionSpec:
        """Factory: create typed DeleteSecurityGroupEnisActionSpec."""
