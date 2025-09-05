"""Delete IAM users and clean up associated IAM resources."""

from typing import Any
from pydantic import Field, model_validator
from botocore.exceptions import ClientError

import core_logging as log

from core_framework.models import DeploymentDetails, ActionResource, ActionSpec

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


class DeleteUserActionSpec(ActionSpec):
    """Parameters for deleting IAM users.

    Attributes:
      account: AWS account ID where users exist.
      region: AWS region for IAM operations.
      user_names: List of IAM user names to delete.
    """

    user_names: list[str] = Field(
        ...,
        alias="UserNames",
        description="The list of users to delete (required)",
    )

    @property
    def user_name(self) -> str:
        """First user name for backward compatibility."""
        return self.user_names[0] if self.user_names else ""

    @model_validator(mode="before")
    @classmethod
    def validate_user_names(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Normalize and validate user_names into a list.

        - Accepts UserNames (list) or UserName (single string).
        - Ensures the final field is a list of names.
        """
        # Handle both UserNames and UserName parameters
        for lk in ["UserNames", "user_names"]:
            if lk in values:
                break
        else:
            lk = "UserNames"
            values[lk] = []

        users = values.get(lk, [])

        # Handle single UserName parameter
        for nk in ["UserName", "user_name"]:
            if nk in values:
                users.append(values[nk])
                del values[nk]

        values[lk] = users
        return values


class DeleteUserActionResource(ActionResource):
    """Resource model for DeleteUser (normalizes kind/spec)."""

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Normalize incoming values and enforce canonical kind/spec."""
        if not isinstance(values, dict):
            return values

        values.pop("kind", None)
        values.pop("Kind", None)
        values["kind"] = "AWS::DeleteUser"

        spec = values.pop("spec", None) or values.pop("Spec", None)
        if isinstance(spec, dict):
            values["spec"] = spec
        elif isinstance(spec, DeleteUserActionSpec):
            values["spec"] = spec.model_dump()

        return values


class DeleteUserAction(BaseAction):
    """Delete IAM users and all related IAM resources.

    Cleans up access keys, signing certificates, group memberships,
    inline/managed policies, login profiles, MFA devices, SSH keys,
    and service-specific credentials before deleting the user.
    """

    def __init__(
        self,
        definition: ActionResource,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        """Initialize the action and validate parameters.

        Args:
          definition: Action resource with metadata/spec.
          context: Rendering context for templates.
          deployment_details: Deployment metadata.

        """
        super().__init__(definition, context, deployment_details)

        # Validate the parameters
        self.params = DeleteUserActionSpec(**definition.spec)

    def _resolve(self):
        """Render templates for account, region, and user_names."""
        log.trace("Resolving DeleteUserAction")

        self.params.account = self.renderer.render_string(self.params.account, self.context)
        self.params.region = self.renderer.render_string(self.params.region, self.context)

        for i, user_name in enumerate(self.params.user_names):
            self.params.user_names[i] = self.renderer.render_string(user_name, self.context)

        log.trace("DeleteUserAction resolved")

    def _execute(self):
        """Delete specified IAM users and dependencies, set results.

        Sets state/outputs for deleted, failed, and skipped users.
        """
        log.trace("Executing DeleteUserAction")

        # Validate required parameters
        if not self.params.user_names:
            self.set_failed("UserNames parameter is required and must contain at least one user")
            log.error("UserNames parameter is required and must contain at least one user")
            return

        # Set initial state information
        self.set_state("Account", self.params.account)
        self.set_state("Region", self.params.region)
        self.set_state("UserNames", self.params.user_names)
        self.set_state("DeletionStarted", True)
        self.set_state("StartTime", util.get_current_timestamp())

        # Set outputs for other actions to reference
        self.set_output("Account", self.params.account)
        self.set_output("Region", self.params.region)
        self.set_output("UserNames", self.params.user_names)
        self.set_output("DeletionStarted", True)

        # Obtain an IAM client
        try:
            iam_client = aws.iam_client(
                region=self.params.region,
                role=util.get_provisioning_role_arn(self.params.account),
            )
        except Exception as e:
            log.error("Failed to create IAM client: {}", e)
            self.set_failed(f"Failed to create IAM client: {e}")
            return

        # Track deletion results
        deleted_users = []
        failed_users = []
        skipped_users = []

        # Process each user
        for user_name in self.params.user_names:
            log.info("Processing user '{}'", user_name)

            try:
                # Check if user exists
                user_exists = self._check_user_exists(iam_client, user_name)

                if not user_exists:
                    log.warning("User '{}' does not exist, skipping", user_name)
                    skipped_users.append({"UserName": user_name, "Reason": "User does not exist"})
                    continue

                # Delete user and all associated resources
                self._delete_user_completely(iam_client, user_name)

                log.info("Successfully deleted user '{}'", user_name)
                deleted_users.append(user_name)

            except ClientError as e:
                error_code = e.response["Error"]["Code"]
                error_message = e.response["Error"]["Message"]

                log.error(
                    "Failed to delete user '{}': {} - {}",
                    user_name,
                    error_code,
                    error_message,
                )
                failed_users.append(
                    {
                        "UserName": user_name,
                        "ErrorCode": error_code,
                        "ErrorMessage": error_message,
                    }
                )

            except Exception as e:
                log.error("Unexpected error deleting user '{}': {}", user_name, e)
                failed_users.append(
                    {
                        "UserName": user_name,
                        "ErrorCode": "UnexpectedError",
                        "ErrorMessage": str(e),
                    }
                )

        # Set completion state
        self.set_state("DeletionCompleted", True)
        self.set_state("CompletionTime", util.get_current_timestamp())
        self.set_state("DeletedUsers", deleted_users)
        self.set_state("FailedUsers", failed_users)
        self.set_state("SkippedUsers", skipped_users)

        # Set outputs
        self.set_output("DeletionCompleted", True)
        self.set_output("DeletedUsers", deleted_users)
        self.set_output("FailedUsers", failed_users)
        self.set_output("SkippedUsers", skipped_users)

        # Determine overall result
        if failed_users:
            self.set_state("DeletionResult", "PARTIAL_FAILURE")
            self.set_output("DeletionResult", "PARTIAL_FAILURE")
            self.set_failed(f"Failed to delete {len(failed_users)} out of {len(self.params.user_names)} users")
        else:
            self.set_state("DeletionResult", "SUCCESS")
            self.set_output("DeletionResult", "SUCCESS")

            if skipped_users and not deleted_users:
                self.set_complete(f"All {len(skipped_users)} users were already deleted or did not exist")
            else:
                self.set_complete(
                    f"Successfully processed {len(self.params.user_names)} users: {len(deleted_users)} deleted, {len(skipped_users)} skipped"
                )

        log.trace("DeleteUserAction execution completed")

    def _check(self):
        """No-op; IAM user deletion is immediate."""
        log.trace("Checking DeleteUserAction")

        self.set_complete("User deletion operations are immediate")

        log.trace("DeleteUserAction check completed")

    def _unexecute(self):
        """No rollback; deleted IAM users cannot be restored."""
        log.trace("Unexecuting DeleteUserAction")

        # User deletion cannot be undone
        log.warning("User deletion cannot be rolled back - deleted users cannot be restored")

        deleted_users = self.get_state("DeletedUsers", [])
        if deleted_users:
            log.warning(
                "The following users were deleted and cannot be restored: {}",
                deleted_users,
            )

        self.set_state("RollbackAttempted", True)
        self.set_state("RollbackResult", "NOT_POSSIBLE")

        self.set_complete("User deletion cannot be rolled back")

        log.trace("DeleteUserAction unexecution completed")

    def _cancel(self):
        """No-op; user deletion is immediate and cannot be cancelled."""
        log.trace("Cancelling DeleteUserAction")

        # User deletion is immediate and cannot be cancelled
        self.set_complete("User deletion operations are immediate and cannot be cancelled")

        log.trace("DeleteUserAction cancellation completed")

    def _check_user_exists(self, iam_client, user_name: str) -> bool:
        """Return True if the IAM user exists.

        Args:
          iam_client: Boto3 IAM client.
          user_name: Name of the user to check.

        Returns:
          True if user exists, else False.

        Raises:
          ClientError: For non-NoSuchEntity API errors.
        """
        try:
            iam_client.get_user(UserName=user_name)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchEntity":
                return False
            else:
                # Re-raise other errors
                raise

    def _delete_user_completely(self, iam_client, user_name: str):
        """Delete a user and all associated IAM resources.

        Args:
          iam_client: Boto3 IAM client.
          user_name: Name of the user to delete.

        Raises:
          ClientError: If any IAM operation fails.
        """
        log.debug("Deleting user '{}' and all associated resources", user_name)

        # 1. Delete signing certificates
        try:
            response = iam_client.list_signing_certificates(UserName=user_name)
            for certificate in response["Certificates"]:
                log.debug("Deleting signing certificate '{}'", certificate["CertificateId"])
                iam_client.delete_signing_certificate(
                    UserName=user_name,
                    CertificateId=certificate["CertificateId"],
                )
        except ClientError as e:
            log.warning("Failed to delete signing certificates for user '{}': {}", user_name, e)

        # 2. Remove user from groups
        try:
            response = iam_client.list_groups_for_user(UserName=user_name)
            for group in response["Groups"]:
                log.debug("Removing user '{}' from group '{}'", user_name, group["GroupName"])
                iam_client.remove_user_from_group(UserName=user_name, GroupName=group["GroupName"])
        except ClientError as e:
            log.warning("Failed to remove user '{}' from groups: {}", user_name, e)

        # 3. Delete inline user policies
        try:
            response = iam_client.list_user_policies(UserName=user_name)
            for policy_name in response["PolicyNames"]:
                log.debug("Deleting inline policy '{}' from user '{}'", policy_name, user_name)
                iam_client.delete_user_policy(UserName=user_name, PolicyName=policy_name)
        except ClientError as e:
            log.warning("Failed to delete inline policies for user '{}': {}", user_name, e)

        # 4. Detach managed user policies
        try:
            response = iam_client.list_attached_user_policies(UserName=user_name)
            for policy in response["AttachedPolicies"]:
                log.debug(
                    "Detaching managed policy '{}' from user '{}'",
                    policy["PolicyArn"],
                    user_name,
                )
                iam_client.detach_user_policy(UserName=user_name, PolicyArn=policy["PolicyArn"])
        except ClientError as e:
            log.warning("Failed to detach managed policies for user '{}': {}", user_name, e)

        # 5. Delete access keys
        try:
            response = iam_client.list_access_keys(UserName=user_name)
            for access_key in response["AccessKeyMetadata"]:
                log.debug(
                    "Deleting access key '{}' for user '{}'",
                    access_key["AccessKeyId"],
                    user_name,
                )
                iam_client.delete_access_key(UserName=user_name, AccessKeyId=access_key["AccessKeyId"])
        except ClientError as e:
            log.warning("Failed to delete access keys for user '{}': {}", user_name, e)

        # 6. Delete login profile (console password)
        try:
            iam_client.delete_login_profile(UserName=user_name)
            log.debug("Deleted login profile for user '{}'", user_name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchEntity":
                log.debug("User '{}' has no login profile", user_name)
            else:
                log.warning("Failed to delete login profile for user '{}': {}", user_name, e)

        # 7. Delete MFA devices
        try:
            response = iam_client.list_mfa_devices(UserName=user_name)
            for device in response["MFADevices"]:
                log.debug(
                    "Deactivating MFA device '{}' for user '{}'",
                    device["SerialNumber"],
                    user_name,
                )
                iam_client.deactivate_mfa_device(UserName=user_name, SerialNumber=device["SerialNumber"])
        except ClientError as e:
            log.warning("Failed to deactivate MFA devices for user '{}': {}", user_name, e)

        # 8. Delete SSH public keys
        try:
            response = iam_client.list_ssh_public_keys(UserName=user_name)
            for key in response["SSHPublicKeys"]:
                log.debug(
                    "Deleting SSH public key '{}' for user '{}'",
                    key["SSHPublicKeyId"],
                    user_name,
                )
                iam_client.delete_ssh_public_key(UserName=user_name, SSHPublicKeyId=key["SSHPublicKeyId"])
        except ClientError as e:
            log.warning("Failed to delete SSH public keys for user '{}': {}", user_name, e)

        # 9. Delete service-specific credentials
        try:
            response = iam_client.list_service_specific_credentials(UserName=user_name)
            for credential in response["ServiceSpecificCredentials"]:
                log.debug(
                    "Deleting service-specific credential '{}' for user '{}'",
                    credential["ServiceSpecificCredentialId"],
                    user_name,
                )
                iam_client.delete_service_specific_credential(
                    UserName=user_name,
                    ServiceSpecificCredentialId=credential["ServiceSpecificCredentialId"],
                )
        except ClientError as e:
            log.warning(
                "Failed to delete service-specific credentials for user '{}': {}",
                user_name,
                e,
            )

        # 10. Finally, delete the user
        log.debug("Deleting IAM user '{}'", user_name)
        iam_client.delete_user(UserName=user_name)
        log.info("Successfully deleted IAM user '{}'", user_name)

    @classmethod
    def generate_action_resource(cls, **kwargs) -> DeleteUserActionResource:
        """Factory: create a typed DeleteUserActionResource."""
        return DeleteUserActionResource(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> DeleteUserActionSpec:
        """Factory: create a typed DeleteUserActionSpec."""
        return DeleteUserActionSpec(**kwargs)
