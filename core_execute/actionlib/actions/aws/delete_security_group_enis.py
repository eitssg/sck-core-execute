"""Delete ENIs attached to a security group"""

from typing import Any
from pydantic import Field, model_validator
from botocore.exceptions import ClientError

import core_logging as log

from core_framework.models import DeploymentDetails, ActionSpec, ActionParams

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


# If this account is hyperplane enabled, amazon manages the ENI attachments for you.
ENI_OWNER_HYPERPLANE = "amazon-aws"


class DeleteSecurityGroupEnisActionParams(ActionParams):
    """
    Parameters for the DeleteSecurityGroupEnisAction.

    :param account: The account to use for the action (required)
    :type account: str
    :param region: The region where the security group is located (required)
    :type region: str
    :param security_group_id: The ID of the security group to delete ENIs from (required)
    :type security_group_id: str
    """

    security_group_id: str = Field(
        ...,
        alias="SecurityGroupId",
        description="The ID of the security group to delete ENIs from (required)",
    )


class DeleteSecurityGroupEnisActionSpec(ActionSpec):
    """
    Generate the action definition for DeleteSecurityGroupEnisAction.

    This class provides default values and validation for DeleteSecurityGroupEnisAction parameters.

    :param values: Dictionary of action specification values
    :type values: dict[str, Any]
    :return: Validated action specification values
    :rtype: dict[str, Any]
    """

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """
        Validate the parameters for the DeleteSecurityGroupEnisActionSpec.

        :param values: Input values for validation
        :type values: dict[str, Any]
        :return: Validated and potentially modified values
        :rtype: dict[str, Any]
        """
        if not (values.get("name") or values.get("Name")):
            values["name"] = "action-aws-deletesecuritygroupenis-name"
        if not (values.get("kind") or values.get("Kind")):
            values["kind"] = "AWS::DeleteSecurityGroupEnis"
        if not values.get(
            "depends_on", values.get("DependsOn")
        ):  # arrays are falsy if empty
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Spec")):
            values["params"] = {
                "account": "",
                "region": "",
                "security_group_id": "",
            }
        return values


class DeleteSecurityGroupEnisAction(BaseAction):
    """
    Delete ENIs attached to a security group.

    This action will delete ENIs attached to a security group. The action will
    detach in-use ENIs and delete available ENIs, handling hyperplane-managed
    ENIs appropriately.

    :param definition: The action specification containing configuration details
    :type definition: ActionSpec
    :param context: The Jinja2 rendering context containing all variables
    :type context: dict[str, Any]
    :param deployment_details: Client/portfolio/app/branch/build information
    :type deployment_details: DeploymentDetails

    .. rubric:: Parameters

    :Name: Enter a name to define this action instance
    :Kind: Use the value ``AWS::DeleteSecurityGroupEnis``
    :Spec.Account: The account where the security group is located (required)
    :Spec.Region: The region where the security group is located (required)
    :Spec.SecurityGroupId: The ID of the security group to delete ENIs from (required)

    .. rubric:: ActionSpec Example

    .. code-block:: yaml

        - Name: action-aws-deletesecuritygroupenis-name
          Kind: "AWS::DeleteSecurityGroupEnis"
          Spec:
            Account: "154798051514"
            Region: "ap-southeast-1"
            SecurityGroupId: "sg-1234567890abcdef0"
          Scope: "build"

    .. note::
        The action handles hyperplane-managed ENIs by waiting for AWS to detach them.

    .. warning::
        ENI deletion is irreversible and may affect running instances.
    """

    def __init__(
        self,
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        # Validate and set the parameters
        self.params = DeleteSecurityGroupEnisActionParams(**definition.params)

    def _resolve(self):
        """
        Resolve template variables in action parameters.

        This method renders Jinja2 templates in the action parameters using the current context.
        """
        log.trace("Resolving DeleteSecurityGroupEnisAction")

        self.params.account = self.renderer.render_string(
            self.params.account, self.context
        )
        self.params.region = self.renderer.render_string(
            self.params.region, self.context
        )
        self.params.security_group_id = self.renderer.render_string(
            self.params.security_group_id, self.context
        )

        log.trace("DeleteSecurityGroupEnisAction resolved")

    def _execute(self):
        """
        Execute the ENI deletion operation.

        This method initiates the deletion of ENIs attached to the specified security group.

        :raises: Sets action to failed if security group ID is missing or EC2 operation fails
        """
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

        self.set_running(
            f"Deleting ENIs attached to security group '{self.params.security_group_id}'"
        )
        self._detach_enis()

        log.trace("DeleteSecurityGroupEnisAction execution completed")

    def _check(self):
        """
        Check the status of the ENI deletion operation.

        This method continues the ENI deletion process by checking for remaining ENIs
        and processing them accordingly.
        """
        log.trace("Checking DeleteSecurityGroupEnisAction")

        self._detach_enis()

        log.trace("DeleteSecurityGroupEnisAction check completed")

    def _unexecute(self):
        """
        Rollback the ENI deletion operation.

        .. note::
            ENI deletion cannot be undone. This method is a no-op.
        """
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
        """
        Cancel the ENI deletion operation.

        .. note::
            ENI deletion operations in progress cannot be cancelled.
        """
        log.trace("Cancelling DeleteSecurityGroupEnisAction")

        # ENI operations cannot be cancelled once started
        self.set_complete("ENI deletion operations cannot be cancelled")

        log.trace("DeleteSecurityGroupEnisAction cancellation completed")

    def _detach_enis(self):
        """
        Detach and delete ENIs attached to the security group.

        This method handles the core logic of finding, detaching, and deleting ENIs
        while properly handling different ENI states and error conditions.
        """
        log.trace(
            "Processing ENIs for security group '{}'", self.params.security_group_id
        )

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
                Filters=[
                    {"Name": "group-id", "Values": [self.params.security_group_id]}
                ]
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

                self.set_complete(
                    f"Security group '{self.params.security_group_id}' not found, no ENIs to delete"
                )
                return
            else:
                log.error(
                    "Error describing network interfaces for security group '{}': {} - {}",
                    self.params.security_group_id,
                    error_code,
                    error_message,
                )
                self.set_failed(
                    f"Failed to describe network interfaces: {error_message}"
                )
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

                self.set_complete(
                    f"No ENIs found attached to security group '{self.params.security_group_id}'"
                )
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
                eni_id == item.get("EniId")
                for item in (detached_enis + deleted_enis + skipped_enis + failed_enis)
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

                        log.debug(
                            "Successfully deleted previously detached ENI '{}'", eni_id
                        )

                    except ClientError as e:
                        error_code = e.response["Error"]["Code"]
                        error_message = e.response["Error"]["Message"]

                        log.error(
                            "Error deleting previously detached ENI '{}': {} - {}",
                            eni_id,
                            error_code,
                            error_message,
                        )
                        failed_enis.append(
                            {"EniId": eni_id, "Error": f"{error_code}: {error_message}"}
                        )
                        in_use_enis.remove(
                            eni_id
                        )  # Remove from in_use tracking even on failure

                    except Exception as e:
                        log.error(
                            "Unexpected error deleting previously detached ENI '{}': {}",
                            eni_id,
                            e,
                        )
                        failed_enis.append({"EniId": eni_id, "Error": str(e)})
                        in_use_enis.remove(
                            eni_id
                        )  # Remove from in_use tracking even on failure

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
                            log.warning(
                                "ENI '{}' is in-use but has no attachment ID", eni_id
                            )
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
                failed_enis.append(
                    {"EniId": eni_id, "Error": f"{error_code}: {error_message}"}
                )

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
            self.set_running(
                f"Waiting for {len(in_use_enis)} detached ENIs to become available for deletion"
            )
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
    def generate_action_spec(cls, **kwargs) -> DeleteSecurityGroupEnisActionSpec:
        return DeleteSecurityGroupEnisActionSpec(**kwargs)

    @classmethod
    def generate_action_parameters(
        cls, **kwargs
    ) -> DeleteSecurityGroupEnisActionParams:
        return DeleteSecurityGroupEnisActionParams(**kwargs)
