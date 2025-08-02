"""Duplicate an AMI and copy it to one or more AWS accounts"""

from typing import Any
from pydantic import Field, model_validator
from botocore.exceptions import ClientError

import core_logging as log

from core_framework.models import DeploymentDetails, ActionSpec, ActionParams

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


class DuplicateImageToAccountActionParams(ActionParams):
    """
    Parameters for the DuplicateImageToAccountAction.

    :param account: The source account where the image is located (required)
    :type account: str
    :param region: The region where the image operations will occur (required)
    :type region: str
    :param image_name: The name of the AMI to duplicate (required)
    :type image_name: str
    :param accounts_to_share: List of target accounts to copy the image to (required)
    :type accounts_to_share: list[str]
    :param kms_key_arn: The KMS key ARN to use for encryption in target accounts (required)
    :type kms_key_arn: str
    :param tags: Additional tags to apply to the copied images (optional)
    :type tags: dict[str, str] | None
    """

    image_name: str = Field(
        ...,
        alias="ImageName",
        description="The name of the AMI to duplicate (required)",
    )
    accounts_to_share: list[str] = Field(
        ...,
        alias="AccountsToShare",
        description="List of target accounts to copy the image to (required)",
    )
    kms_key_arn: str = Field(
        ...,
        alias="KmsKeyArn",
        description="The KMS key ARN to use for encryption in target accounts (required)",
    )
    tags: dict[str, str] | None = Field(
        default_factory=dict,
        alias="Tags",
        description="Additional tags to apply to the copied images (optional)",
    )


class DuplicateImageToAccountActionSpec(ActionSpec):
    """
    Generate the action definition for DuplicateImageToAccountAction.

    This class provides default values and validation for DuplicateImageToAccountAction parameters.

    :param values: Dictionary of action specification values
    :type values: dict[str, Any]
    :return: Validated action specification values
    :rtype: dict[str, Any]
    """

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """
        Validate the parameters for the DuplicateImageToAccountActionSpec.

        :param values: Input values for validation
        :type values: dict[str, Any]
        :return: Validated and potentially modified values
        :rtype: dict[str, Any]
        """
        if not (values.get("name") or values.get("Name")):
            values["name"] = "action-aws-duplicateimagetoaccount-name"
        if not (values.get("kind") or values.get("Kind")):
            values["kind"] = "AWS::DuplicateImageToAccount"
        if not values.get("depends_on", values.get("DependsOn")):  # arrays are falsy if empty
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Params")):
            values["params"] = {
                "account": "",
                "region": "",
                "image_name": "",
                "accounts_to_share": [],
                "kms_key_arn": "",
                "tags": {},
            }

        return values


class DuplicateImageToAccountAction(BaseAction):
    """
    Duplicate an AMI and copy it to one or more AWS accounts.

    This action duplicates an existing AMI from a source account and creates copies
    in one or more target accounts. The process involves:

    1. Finding the source AMI and its snapshots
    2. Sharing snapshots with target accounts
    3. Copying snapshots in each target account with encryption
    4. Creating new AMIs from the copied snapshots
    5. Applying tags to the new AMIs and snapshots

    :param definition: The action specification containing configuration details
    :type definition: ActionSpec
    :param context: The Jinja2 rendering context containing all variables
    :type context: dict[str, Any]
    :param deployment_details: Client/portfolio/app/branch/build information
    :type deployment_details: DeploymentDetails

    .. rubric:: Parameters

    :Name: Enter a name to define this action instance
    :Kind: Use the value ``AWS::DuplicateImageToAccount``
    :Params.Account: The source account where the AMI is located (required)
    :Params.Region: The region where the AMI operations will occur (required)
    :Params.ImageName: The name of the AMI to duplicate (required)
    :Params.AccountsToShare: List of target accounts to copy the AMI to (required)
    :Params.KmsKeyArn: The KMS key ARN for encryption in target accounts (required)
    :Params.Tags: Additional tags to apply to copied AMIs (optional)

    .. rubric:: ActionSpec Example

    .. code-block:: yaml

        - Name: action-aws-duplicateimagetoaccount-name
          Kind: "AWS::DuplicateImageToAccount"
          Params:
            Account: "154798051514"
            Region: "ap-southeast-1"
            ImageName: "my-application-ami-v1.0"
            AccountsToShare: ["123456789012", "123456789013"]
            KmsKeyArn: "arn:aws:kms:ap-southeast-1:154798051514:key/your-kms-key-id"
            Tags:
              From: "John Smith"
              Purpose: "Cross-account deployment"
          Scope: "build"

    .. note::
        The provisioning role must exist and be trusted in all target accounts.
        KMS key permissions must allow cross-account usage.

    .. warning::
        AMI copying can take significant time depending on the size of the underlying snapshots.
        This action runs within AWS Step Functions and will continuously loop through _check()
        until all AMI operations complete. The state file tracks which accounts have completed,
        so if Lambda times out, Step Functions will restart execution and continue monitoring
        remaining accounts. Progress is preserved across Lambda invocations.
    """

    def __init__(
        self,
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        # Validate the action parameters
        self.params = DuplicateImageToAccountActionParams(**definition.params)

        if deployment_details.delivered_by:
            self.params.tags["DeliveredBy"] = deployment_details.delivered_by

    def _resolve(self):
        """
        Resolve template variables in action parameters.

        This method renders Jinja2 templates in the action parameters using the current context.
        It handles account IDs, image names, region, and KMS key ARNs.
        """
        log.trace("Resolving DuplicateImageToAccountAction")

        self.params.account = self.renderer.render_string(self.params.account, self.context)
        self.params.image_name = self.renderer.render_string(self.params.image_name, self.context)
        self.params.region = self.renderer.render_string(self.params.region, self.context)
        self.params.kms_key_arn = self.renderer.render_string(self.params.kms_key_arn, self.context)

        # Resolve each account in the list
        if isinstance(self.params.accounts_to_share, list):
            for i, account in enumerate(self.params.accounts_to_share):
                self.params.accounts_to_share[i] = self.renderer.render_string(account, self.context)

        log.trace("DuplicateImageToAccountAction resolved")

    def _execute(self):
        """
        Execute the AMI duplication operation.

        This method performs the complete AMI duplication workflow but is designed to be idempotent.
        When Step Functions restarts Lambda, this method will be called again and should:
        1. Check for existing operations in progress
        2. Resume from where it left off
        3. Only start new operations for accounts not yet processed

        :raises: Sets action to failed if any critical operation fails
        """
        log.trace("Executing DuplicateImageToAccountAction")

        # Check if this is a restart/resume scenario
        duplication_started = self.get_state("DuplicationStarted", False)
        if duplication_started:
            log.info("Resuming AMI duplication operation - checking for work in progress")
            return self._resume_execution()

        # Fresh execution - validate required parameters
        if not self.params.accounts_to_share:
            self.set_complete("No target accounts specified for AMI duplication")
            log.warning("No target accounts specified for AMI duplication")
            return

        # Set initial state information
        self.set_state("SourceAccount", self.params.account)
        self.set_state("SourceRegion", self.params.region)
        self.set_state("SourceImageName", self.params.image_name)
        self.set_state("TargetAccounts", self.params.accounts_to_share)
        self.set_state("KmsKeyArn", self.params.kms_key_arn)
        self.set_state("DuplicationStarted", True)
        self.set_state("StartTime", util.get_current_timestamp())

        # Set outputs for other actions to reference
        self.set_output("SourceAccount", self.params.account)
        self.set_output("SourceRegion", self.params.region)
        self.set_output("SourceImageName", self.params.image_name)
        self.set_output("TargetAccounts", self.params.accounts_to_share)
        self.set_output("DuplicationStarted", True)

        try:
            # Obtain an EC2 client for the source account
            ec2_client = aws.ec2_client(
                region=self.params.region,
                role=util.get_provisioning_role_arn(self.params.account),
            )

            # Find the source AMI (only if not already found)
            source_image_id = self.get_state("SourceImageId", "")
            if not source_image_id:
                source_image_id, snapshot_ids = self._find_source_image(ec2_client)
                if not source_image_id:
                    return  # Error already set in _find_source_image

                self.set_state("SourceImageId", source_image_id)
                self.set_state("SourceSnapshotIds", snapshot_ids)
                self.set_output("SourceImageId", source_image_id)
            else:
                snapshot_ids = self.get_state("SourceSnapshotIds", [])
                log.info(
                    "Using existing source AMI '{}' with snapshots: {}",
                    source_image_id,
                    snapshot_ids,
                )

            # Start processing accounts
            self._process_target_accounts(ec2_client, source_image_id, snapshot_ids)

        except Exception as e:
            log.error("Critical error during AMI duplication: {}", e)
            self.set_failed(f"Critical error during AMI duplication: {e}")

        log.trace("DuplicateImageToAccountAction execution completed")

    def _resume_execution(self):
        """
        Resume execution when Step Functions restarts Lambda.

        This method checks the current state and continues processing accounts
        that haven't been started yet, while leaving in-progress operations alone.
        """
        log.trace("Resuming DuplicateImageToAccountAction execution")

        # Get existing state
        source_image_id = self.get_state("SourceImageId", "")
        snapshot_ids = self.get_state("SourceSnapshotIds", [])

        if not source_image_id:
            log.error("Resume called but no source image ID found in state")
            self.set_failed("Cannot resume: missing source image information")
            return

        try:
            # Obtain an EC2 client for the source account
            ec2_client = aws.ec2_client(
                region=self.params.region,
                role=util.get_provisioning_role_arn(self.params.account),
            )

            # Continue processing accounts
            self._process_target_accounts(ec2_client, source_image_id, snapshot_ids)

        except Exception as e:
            log.error("Critical error during resumed AMI duplication: {}", e)
            self.set_failed(f"Critical error during resumed AMI duplication: {e}")

        log.trace("DuplicateImageToAccountAction resume completed")

    def _process_target_accounts(self, ec2_client, source_image_id: str, snapshot_ids: list[str]):
        """
        Process target accounts, handling both fresh starts and resumes.

        This method is idempotent and will:
        - Skip accounts that already have AMIs created
        - Continue processing accounts that haven't been started
        - Handle partial failures gracefully

        :param ec2_client: EC2 client for the source account
        :type ec2_client: boto3.client
        :param source_image_id: ID of the source AMI
        :type source_image_id: str
        :param snapshot_ids: List of snapshot IDs from the source AMI
        :type snapshot_ids: list[str]
        """
        log.debug("Processing target accounts for AMI duplication")

        # Get existing state
        successful_accounts = self.get_state("SuccessfulAccounts", [])
        failed_accounts = self.get_state("FailedAccounts", [])
        created_images = self.get_state("CreatedImages", {})
        in_progress_accounts = self.get_state("InProgressAccounts", [])

        # Determine which accounts still need processing
        processed_accounts = set(successful_accounts + [fa["Account"] for fa in failed_accounts])
        accounts_to_process = [acc for acc in self.params.accounts_to_share if acc not in processed_accounts]

        log.info(
            "Account status: {} successful, {} failed, {} in progress, {} remaining to process",
            len(successful_accounts),
            len(failed_accounts),
            len(in_progress_accounts),
            len(accounts_to_process),
        )

        # Process each remaining target account
        for target_account in accounts_to_process:
            log.info("Processing target account '{}'", target_account)

            # Check if this account already has an AMI being created
            existing_image_id = self.get_state(f"ImageId_{target_account}", None)
            if existing_image_id:
                log.info(
                    "Account '{}' already has AMI '{}' in progress, skipping duplication",
                    target_account,
                    existing_image_id,
                )
                if target_account not in successful_accounts:
                    successful_accounts.append(target_account)
                    created_images[target_account] = existing_image_id
                continue

            try:
                # Mark account as in progress
                if target_account not in in_progress_accounts:
                    in_progress_accounts.append(target_account)
                    self.set_state("InProgressAccounts", in_progress_accounts)

                # Duplicate AMI to this account
                target_image_id = self._duplicate_to_account(ec2_client, source_image_id, snapshot_ids[0], target_account)

                # Mark as successful
                successful_accounts.append(target_account)
                created_images[target_account] = target_image_id
                self.set_state(f"ImageId_{target_account}", target_image_id)
                self.set_state(
                    f"DuplicationStartTime_{target_account}",
                    util.get_current_timestamp(),
                )

                # Remove from in-progress list
                if target_account in in_progress_accounts:
                    in_progress_accounts.remove(target_account)

                log.info(
                    "Successfully duplicated AMI to account '{}': {}",
                    target_account,
                    target_image_id,
                )

            except Exception as e:
                log.error("Failed to duplicate AMI to account '{}': {}", target_account, e)

                # Remove from in-progress and add to failed
                if target_account in in_progress_accounts:
                    in_progress_accounts.remove(target_account)

                failed_accounts.append(
                    {
                        "Account": target_account,
                        "Error": str(e),
                        "ErrorType": type(e).__name__,
                        "FailureTime": util.get_current_timestamp(),
                    }
                )
                continue

            # Update state after each account to preserve progress
            self.set_state("SuccessfulAccounts", successful_accounts)
            self.set_state("FailedAccounts", failed_accounts)
            self.set_state("CreatedImages", created_images)
            self.set_state("InProgressAccounts", in_progress_accounts)

        # Add this after processing all accounts:
        if not successful_accounts and not failed_accounts:
            self.set_failed("No accounts were processed successfully")
            return

        # Set final completion state
        self.set_state("DuplicationCompleted", True)
        self.set_state("CompletionTime", util.get_current_timestamp())
        self.set_state("SuccessfulAccounts", successful_accounts)
        self.set_state("FailedAccounts", failed_accounts)
        self.set_state("CreatedImages", created_images)
        self.set_state("InProgressAccounts", in_progress_accounts)

        # Set outputs
        self.set_output("DuplicationCompleted", True)
        self.set_output("SuccessfulAccounts", successful_accounts)
        self.set_output("FailedAccounts", failed_accounts)
        self.set_output("CreatedImages", created_images)

        # Determine overall result
        if failed_accounts and not successful_accounts:
            self.set_failed(f"Failed to duplicate AMI to all {len(failed_accounts)} target accounts")
        elif failed_accounts:
            # Partial success - continue with _check() to monitor remaining AMIs
            self.set_state("DuplicationResult", "PARTIAL_SUCCESS")
            self.set_output("DuplicationResult", "PARTIAL_SUCCESS")
            # No explicit flow control set - defaults to "execute" to continue monitoring
        else:
            self.set_state("DuplicationResult", "SUCCESS")
            self.set_output("DuplicationResult", "SUCCESS")
            # No explicit flow control set - defaults to "execute" to continue with _check()

    def _duplicate_to_account(
        self,
        source_ec2_client,
        source_image_id: str,
        snapshot_id: str,
        target_account: str,
    ) -> str:
        """
        Duplicate an AMI to a specific target account.

        This method is designed to be resumable - if called multiple times for the same account,
        it should detect existing work and not duplicate efforts.

        :param source_ec2_client: EC2 client for the source account
        :type source_ec2_client: boto3.client
        :param source_image_id: ID of the source AMI
        :type source_image_id: str
        :param snapshot_id: ID of the source snapshot to copy
        :type snapshot_id: str
        :param target_account: Target account ID
        :type target_account: str
        :return: ID of the created AMI in the target account
        :rtype: str
        :raises ClientError: If any step of the duplication fails
        """
        log.info("Duplicating AMI '{}' to account '{}'", source_image_id, target_account)

        # Check if we already have a snapshot being copied for this account
        copied_snapshot_id = self.get_state(f"CopiedSnapshotId_{target_account}", None)
        target_image_id = self.get_state(f"ImageId_{target_account}", None)

        # If we already have an AMI ID, return it
        if target_image_id:
            log.info(
                "AMI '{}' already exists for account '{}', returning existing ID",
                target_image_id,
                target_account,
            )
            return target_image_id

        try:
            # Step 1: Share snapshot with target account (idempotent operation)
            self.set_running(f"Sharing snapshot with target account {target_account}")
            log.debug("Sharing snapshot '{}' with account '{}'", snapshot_id, target_account)

            source_ec2_client.modify_snapshot_attribute(
                Attribute="createVolumePermission",
                OperationType="add",
                SnapshotId=snapshot_id,
                UserIds=[target_account],
            )
            log.debug("Successfully shared snapshot with target account {}", target_account)

            # Step 2: Get client and resource for target account
            target_ec2_client = aws.ec2_client(
                region=self.params.region,
                role=util.get_provisioning_role_arn(target_account),
            )

            target_ec2_resource = aws.get_resource(
                "ec2",
                region=self.params.region,
                role=util.get_provisioning_role_arn(target_account),
            )

            # Step 3: Copy snapshot in target account (check if already in progress)
            if not copied_snapshot_id:
                self.set_running(f"Copying snapshot to target account {target_account}")  # Fixed typo

                # FIXED: Use resource for snapshot operations
                shared_snapshot = target_ec2_resource.Snapshot(snapshot_id)
                copy_response = shared_snapshot.copy(
                    SourceRegion=self.params.region,
                    Encrypted=True,
                    KmsKeyId=self.params.kms_key_arn,
                    Description=f"Copy of snapshot {snapshot_id} from account {self.params.account}",
                )

                copied_snapshot_id = copy_response["SnapshotId"]
                self.set_state(f"CopiedSnapshotId_{target_account}", copied_snapshot_id)
                self.set_state(
                    f"SnapshotCopyStartTime_{target_account}",
                    util.get_current_timestamp(),
                )

                log.info(
                    "Started snapshot copy from '{}' to '{}' in account '{}'",
                    snapshot_id,
                    copied_snapshot_id,
                    target_account,
                )
            else:
                log.info(
                    "Using existing snapshot copy '{}' for account '{}'",
                    copied_snapshot_id,
                    target_account,
                )

            # Wait for snapshot to complete
            # FIXED: Use resource for snapshot operations
            copied_snapshot = target_ec2_resource.Snapshot(copied_snapshot_id)

            # Check snapshot status before waiting
            copied_snapshot.reload()
            log.debug("Snapshot '{}' status: {}", copied_snapshot_id, copied_snapshot.state)

            if copied_snapshot.state == "pending":
                log.info(
                    "Waiting for snapshot '{}' to complete in account '{}'",
                    copied_snapshot_id,
                    target_account,
                )
                copied_snapshot.wait_until_completed()
            elif copied_snapshot.state != "completed":
                raise Exception(f"Snapshot {copied_snapshot_id} is in unexpected state: {copied_snapshot.state}")

            log.debug(
                "Snapshot '{}' completed in account '{}'",
                copied_snapshot_id,
                target_account,
            )

            # Step 4: Create AMI from copied snapshot
            self.set_running(f"Creating AMI in target account {target_account}")

            # Get original AMI details for accurate copying using the source EC2 client
            source_images_response = source_ec2_client.describe_images(ImageIds=[source_image_id])
            if not source_images_response["Images"]:
                raise Exception(f"Source AMI '{source_image_id}' not found")

            source_image_data = source_images_response["Images"][0]

            # Get the original block device mapping to preserve settings
            source_block_devices = source_image_data.get("BlockDeviceMappings", [])
            if not source_block_devices:
                raise Exception(f"Source AMI '{source_image_id}' has no block device mappings")

            # Find the root device mapping
            root_device_name = source_image_data.get("RootDeviceName", "/dev/sda1")
            root_block_device = None
            for device in source_block_devices:
                if device.get("DeviceName") == root_device_name:
                    root_block_device = device
                    break

            if not root_block_device or "Ebs" not in root_block_device:
                raise Exception(f"Could not find root EBS device mapping for AMI '{source_image_id}'")

            # Preserve original EBS settings from source AMI
            original_ebs = root_block_device["Ebs"]

            # FIXED: Use original AMI's EBS settings instead of hardcoded assumptions
            register_response = target_ec2_client.register_image(
                Architecture=source_image_data.get("Architecture", "x86_64"),
                RootDeviceName=root_device_name,
                BlockDeviceMappings=[
                    {
                        "DeviceName": root_device_name,
                        "Ebs": {
                            "DeleteOnTermination": original_ebs.get("DeleteOnTermination", True),
                            "SnapshotId": copied_snapshot.snapshot_id,  # Use the copied snapshot
                            "VolumeSize": copied_snapshot.volume_size,  # Use copied snapshot size
                            "VolumeType": original_ebs.get("VolumeType", "gp3"),  # Preserve original volume type
                            "Encrypted": True,  # Force encryption in target account (as per KMS requirement)
                            "Iops": original_ebs.get("Iops"),  # Preserve IOPS if specified
                            "Throughput": original_ebs.get("Throughput"),  # Preserve throughput if specified
                        },
                    },
                ],
                Description=f"Copy of AMI {source_image_id} from account {self.params.account}",
                Name=f"{self.params.image_name}-copy-{target_account}-{util.get_current_timestamp_short()}",
                VirtualizationType=source_image_data.get("VirtualizationType", "hvm"),
                EnaSupport=source_image_data.get("EnaSupport", True),
                SriovNetSupport=source_image_data.get("SriovNetSupport", "simple"),
            )

            # FIXED: Access dict key instead of object attribute
            target_image_id = register_response["ImageId"]
            self.set_state(f"ImageId_{target_account}", target_image_id)
            self.set_state(f"ImageCreationTime_{target_account}", util.get_current_timestamp())

            log.info(
                "Successfully created AMI '{}' in target account '{}'",
                target_image_id,
                target_account,
            )

            return target_image_id

        except Exception as e:
            # Clean up state on failure
            self.set_state(f"FailureTime_{target_account}", util.get_current_timestamp())
            log.error("Failed to duplicate AMI to account '{}': {}", target_account, e)
            raise

    def _check(self):
        """
        Check the status of the AMI duplication operation.

        This method monitors the completion status of AMI creation in all target accounts.
        It waits for all AMIs to become available and applies tags when ready. The method
        tracks completion per account to handle Step Function restarts gracefully.
        """
        log.trace("Checking DuplicateImageToAccountAction")

        successful_accounts = self.get_state("SuccessfulAccounts", [])
        if not successful_accounts:
            self.set_complete("No successful account duplications to check")
            return

        # Get previously completed accounts to avoid reprocessing
        completed_accounts = self.get_state("CompletedAccounts", [])

        # Only check accounts that haven't completed yet
        accounts_to_check = [acc for acc in successful_accounts if acc not in completed_accounts]

        if not accounts_to_check:
            self.set_complete(f"AMI duplication already completed for all {len(successful_accounts)} accounts")
            return

        log.info(
            "Checking AMI status for {} accounts: {}",
            len(accounts_to_check),
            accounts_to_check,
        )

        all_complete = True
        newly_completed = []
        still_pending = []

        # Check each target account that hasn't completed yet
        for target_account in accounts_to_check:
            image_id = self.get_state(f"ImageId_{target_account}", None)
            if not image_id:
                log.warning("No image ID found for account '{}', skipping check", target_account)
                continue

            try:
                # Get EC2 client for target account
                ec2_client = aws.ec2_client(
                    region=self.params.region,
                    role=util.get_provisioning_role_arn(target_account),
                )

                # Check image status
                response = ec2_client.describe_images(ImageIds=[image_id])

                if not response["Images"]:
                    log.error("Image '{}' not found in account '{}'", image_id, target_account)
                    continue

                state = response["Images"][0]["State"]
                log.debug(
                    "Image '{}' in account '{}' is in state '{}'",
                    image_id,
                    target_account,
                    state,
                )

                if state == "available":
                    # Apply tags to image and snapshots
                    self._apply_tags_to_image(ec2_client, image_id, response)
                    newly_completed.append(target_account)
                    self.set_state(f"CompletedTime_{target_account}", util.get_current_timestamp())
                    log.info(
                        "AMI '{}' in account '{}' is now available and tagged",
                        image_id,
                        target_account,
                    )

                elif state == "pending":
                    still_pending.append(target_account)
                    all_complete = False
                    log.debug(
                        "AMI '{}' in account '{}' is still pending",
                        image_id,
                        target_account,
                    )

                else:
                    log.error(
                        "Image '{}' in account '{}' is in unexpected state '{}'",
                        image_id,
                        target_account,
                        state,
                    )
                    all_complete = False

            except ClientError as e:
                log.error("Error checking image status in account '{}': {}", target_account, e)
                still_pending.append(target_account)  # Keep checking this account
                all_complete = False

        # Update cumulative state
        all_completed_accounts = completed_accounts + newly_completed
        all_pending_accounts = [acc for acc in still_pending if acc not in all_completed_accounts]

        self.set_state("CompletedAccounts", all_completed_accounts)
        self.set_state("PendingAccounts", all_pending_accounts)
        self.set_state("LastCheckTime", util.get_current_timestamp())

        # Update outputs
        self.set_output("CompletedAccounts", all_completed_accounts)
        self.set_output("PendingAccounts", all_pending_accounts)

        if newly_completed:
            log.info("Newly completed accounts: {}", newly_completed)

        if all_complete and len(all_completed_accounts) == len(successful_accounts):
            completion_times = {}
            for account in all_completed_accounts:
                completion_time = self.get_state(f"CompletedTime_{account}", None)
                if completion_time:
                    completion_times[account] = completion_time

            self.set_state("AllCompletionTimes", completion_times)
            self.set_output("AllCompletionTimes", completion_times)

            self.set_complete(f"AMI duplication completed successfully for all {len(successful_accounts)} accounts")
        else:
            # Continue checking - Step Functions will call _check() again
            log.debug(
                "AMI duplication in progress: {} complete, {} pending",
                len(all_completed_accounts),
                len(all_pending_accounts),
            )

        log.trace("DuplicateImageToAccountAction check completed")

    def _unexecute(self):
        """
        Rollback the AMI duplication operation.

        .. note::
            AMI duplication cannot be automatically rolled back. Created AMIs and snapshots
            remain in target accounts. Manual cleanup may be required.

        .. warning::
            This action does not automatically delete AMIs that were created in target accounts.
            Use appropriate cleanup actions if rollback is required.
        """
        log.trace("Unexecuting DuplicateImageToAccountAction")

        created_images = self.get_state("CreatedImages", {})
        if created_images:
            log.warning(
                "The following AMIs were created and will not be automatically deleted: {}",
                created_images,
            )

        successful_accounts = self.get_state("SuccessfulAccounts", [])
        if successful_accounts:
            log.warning("AMIs created in accounts: {}", successful_accounts)

        self.set_state("RollbackAttempted", True)
        self.set_state("RollbackResult", "MANUAL_CLEANUP_REQUIRED")

        self.set_complete("AMI duplication cannot be automatically rolled back - manual cleanup required")

        log.trace("DuplicateImageToAccountAction unexecution completed")

    def _cancel(self):
        """
        Cancel the AMI duplication operation.

        .. note::
            AMI duplication involves long-running snapshot copy operations that cannot be cancelled
            once started. Any AMIs already created will remain in place.
        """
        log.trace("Cancelling DuplicateImageToAccountAction")

        # AMI duplication operations cannot be cancelled once snapshot copying has started
        self.set_complete("AMI duplication operations cannot be cancelled once snapshot copying has started")

        log.trace("DuplicateImageToAccountAction cancellation completed")

    def _get_target_session(self, target_account: str):
        """
        Get a boto3 session for the target account using the cached session architecture.

        :param target_account: Target account ID
        :type target_account: str
        :return: Configured session for the target account
        :rtype: Session-like object
        """
        log.trace("Getting session for target account '{}'", target_account)

        # Use aws.assume_role to get credentials and update the cached session
        credentials = aws.assume_role(
            role=util.get_provisioning_role_arn(target_account),
            session_name=f"ami-copy-session-{target_account}",
            region=self.params.region,
        )

        # FIXED: Use aws helper to get session instead of creating new boto3.Session
        # This respects the cached session architecture
        target_session = aws.get_session(region=self.params.region, credentials=credentials)

        log.trace("Successfully got session for target account '{}'", target_account)
        return target_session

    def _apply_tags_to_image(self, ec2_client, image_id: str, describe_response: dict):
        """
        Apply tags to the AMI and its associated snapshots.

        :param ec2_client: EC2 client for the target account
        :type ec2_client: boto3.client
        :param image_id: ID of the AMI to tag
        :type image_id: str
        :param describe_response: Response from describe_images call
        :type describe_response: dict
        """
        if not self.params.tags or len(self.params.tags) == 0:
            log.debug("No tags specified, skipping tag application for image '{}'", image_id)
            return

        log.info("Applying tags to image '{}'", image_id)

        try:
            # Tag the AMI
            ec2_client.create_tags(Resources=[image_id], Tags=aws.transform_tag_hash(self.params.tags))

            # Tag associated snapshots
            snapshot_ids = self._get_image_snapshots(describe_response)
            if snapshot_ids:
                log.info("Applying tags to snapshots: {}", snapshot_ids)
                ec2_client.create_tags(
                    Resources=snapshot_ids,
                    Tags=aws.transform_tag_hash(self.params.tags),
                )

            log.debug(
                "Successfully applied tags to image '{}' and {} snapshots",
                image_id,
                len(snapshot_ids),
            )

        except ClientError as e:
            log.warning("Failed to apply tags to image '{}': {}", image_id, e)
            # Don't fail the action for tagging errors

    def _get_image_snapshots(self, describe_images_response: dict) -> list[str]:
        """
        Extract snapshot IDs from a describe_images response.

        :param describe_images_response: Response from EC2 describe_images call
        :type describe_images_response: dict
        :return: List of snapshot IDs associated with the image
        :rtype: list[str]
        """
        snapshots = []
        for mapping in describe_images_response["Images"][0]["BlockDeviceMappings"]:
            if "Ebs" in mapping and "SnapshotId" in mapping["Ebs"]:
                snapshots.append(mapping["Ebs"]["SnapshotId"])

        return snapshots

    def _find_source_image(self, ec2_client) -> tuple[str | None, list[str]]:
        """
        Find the source AMI and extract its snapshot IDs.

        :param ec2_client: EC2 client for the source account
        :type ec2_client: boto3.client
        :return: Tuple of (image_id, snapshot_ids) or (None, []) if not found
        :rtype: tuple[str | None, list[str]]
        """
        log.debug("Finding AMI with name '{}'", self.params.image_name)

        try:
            response = ec2_client.describe_images(Filters=[{"Name": "name", "Values": [self.params.image_name]}])

            if not response["Images"]:
                self.set_failed(f"Could not find AMI with name '{self.params.image_name}' in source account")
                log.error("Could not find AMI with name '{}'", self.params.image_name)
                return None, []

            image = response["Images"][0]
            image_id = image["ImageId"]

            # Extract snapshot IDs from block device mappings
            snapshot_ids = []
            for block_device_mapping in image["BlockDeviceMappings"]:
                if "Ebs" in block_device_mapping and "SnapshotId" in block_device_mapping["Ebs"]:
                    snapshot_ids.append(block_device_mapping["Ebs"]["SnapshotId"])

            if not snapshot_ids:
                self.set_failed(f"AMI '{image_id}' has no EBS snapshots to copy")
                log.error("AMI '{}' has no EBS snapshots", image_id)
                return None, []

            log.info(
                "Found source AMI '{}' with {} snapshots: {}",
                image_id,
                len(snapshot_ids),
                snapshot_ids,
            )
            return image_id, snapshot_ids

        except ClientError as e:
            self.set_failed(f"Error finding source AMI: {e}")
            log.error("Error finding source AMI: {}", e)
            return None, []

    @classmethod
    def generate_action_spec(cls, **kwargs) -> DuplicateImageToAccountActionSpec:
        return DuplicateImageToAccountActionSpec(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> DuplicateImageToAccountActionParams:
        return DuplicateImageToAccountActionParams(**kwargs)
