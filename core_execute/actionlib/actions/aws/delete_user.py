"""Delete IAM users from an AWS account"""

from typing import Any
from pydantic import Field, model_validator
from botocore.exceptions import ClientError

import core_logging as log

from core_framework.models import DeploymentDetails, ActionSpec, ActionParams

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


class DeleteUserActionParams(ActionParams):
    """
    Parameters for the DeleteUserAction.

    :param account: The account to use for the action (required)
    :type account: str
    :param region: The region where the user is located (required)
    :type region: str
    :param user_names: The list of users to delete (required)
    :type user_names: list[str]
    """

    user_names: list[str] = Field(
        ...,
        alias="UserNames",
        description="The list of users to delete (required)",
    )

    @property
    def user_name(self) -> str:
        """
        Return the first user name for backward compatibility.

        :return: The first user name in the list
        :rtype: str
        """
        return self.user_names[0] if self.user_names else ""

    @model_validator(mode="before")
    @classmethod
    def validate_user_names(cls, values: dict[str, Any]) -> dict[str, Any]:
        """
        Ensure user_names is a list and contains at least one user name.

        :param values: Input values for validation
        :type values: dict[str, Any]
        :return: Validated and potentially modified values
        :rtype: dict[str, Any]
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


class DeleteUserActionSpec(ActionSpec):
    """
    Generate the action definition for DeleteUserAction.

    This class provides default values and validation for DeleteUserAction parameters.

    :param values: Dictionary of action specification values
    :type values: dict[str, Any]
    :return: Validated action specification values
    :rtype: dict[str, Any]
    """

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """
        Validate the parameters for the DeleteUserActionSpec.

        :param values: Input values for validation
        :type values: dict[str, Any]
        :return: Validated and potentially modified values
        :rtype: dict[str, Any]
        """
        if not (values.get("name") or values.get("Name")):
            values["name"] = "action-aws-deleteuser-name"
        if not (values.get("kind") or values.get("Kind")):
            values["kind"] = "AWS::DeleteUser"
        if not values.get("depends_on", values.get("DependsOn")):  # arrays are falsy if empty
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Params")):
            values["params"] = {
                "account": "",
                "region": "",
                "user_names": [],
            }
        return values


class DeleteUserAction(BaseAction):
    """
    Delete IAM users from an AWS account.

    This action will delete one or more IAM users from an AWS account. The action
    will clean up all associated resources including access keys, signing certificates,
    group memberships, and attached policies before deleting the user.

    :param definition: The action specification containing configuration details
    :type definition: ActionSpec
    :param context: The Jinja2 rendering context containing all variables
    :type context: dict[str, Any]
    :param deployment_details: Client/portfolio/app/branch/build information
    :type deployment_details: DeploymentDetails

    .. rubric:: Parameters

    :Name: Enter a name to define this action instance
    :Kind: Use the value ``AWS::DeleteUser``
    :Params.Account: The account where the users are located (required)
    :Params.Region: The region for the IAM operations (required)
    :Params.UserNames: The list of user names to delete (required)

    .. rubric:: ActionSpec Example

    .. code-block:: yaml

        - Name: action-aws-deleteuser-name
          Kind: "AWS::DeleteUser"
          Params:
            Account: "154798051514"
            Region: "us-east-1"
            UserNames: ["john.smith", "jane.doe"]
          Scope: "build"

    .. note::
        User deletion removes all associated resources and cannot be undone.

    .. warning::
        Deleting users will invalidate any credentials they were using.
    """

    def __init__(
        self,
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        # Validate the parameters
        self.params = DeleteUserActionParams(**definition.params)

    def _resolve(self):
        """
        Resolve template variables in action parameters.

        This method renders Jinja2 templates in the action parameters using the current context.
        """
        log.trace("Resolving DeleteUserAction")

        self.params.account = self.renderer.render_string(self.params.account, self.context)
        self.params.region = self.renderer.render_string(self.params.region, self.context)

        for i, user_name in enumerate(self.params.user_names):
            self.params.user_names[i] = self.renderer.render_string(user_name, self.context)

        log.trace("DeleteUserAction resolved")

    def _execute(self):
        """
        Execute the user deletion operation.

        This method deletes the specified IAM users and all their associated resources.
        IAM user deletion is typically fast and doesn't require long-running monitoring.

        :raises: Sets action to failed if user deletion fails
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
        """
        Check the status of the user deletion operation.

        IAM user deletion is typically immediate, so this method just confirms completion.
        """
        log.trace("Checking DeleteUserAction")

        # IAM user deletion is immediate, so if we get here, it's already complete
        self.set_complete("User deletion operations are immediate")

        log.trace("DeleteUserAction check completed")

    def _unexecute(self):
        """
        Rollback the user deletion operation.

        .. note::
            User deletion cannot be undone. This method is a no-op.
        """
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
        """
        Cancel the user deletion operation.

        .. note::
            User deletion operations are immediate and cannot be cancelled.
        """
        log.trace("Cancelling DeleteUserAction")

        # User deletion is immediate and cannot be cancelled
        self.set_complete("User deletion operations are immediate and cannot be cancelled")

        log.trace("DeleteUserAction cancellation completed")

    def _check_user_exists(self, iam_client, user_name: str) -> bool:
        """
        Check if a user exists in IAM.

        :param iam_client: IAM client
        :type iam_client: boto3.client
        :param user_name: Name of the user to check
        :type user_name: str
        :return: True if user exists, False otherwise
        :rtype: bool
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
        """
        Delete a user and all associated resources.

        :param iam_client: IAM client
        :type iam_client: boto3.client
        :param user_name: Name of the user to delete
        :type user_name: str
        :raises ClientError: If any IAM operation fails
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
    def generate_action_spec(cls, **kwargs) -> DeleteUserActionSpec:
        return DeleteUserActionSpec(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> DeleteUserActionParams:
        return DeleteUserActionParams(**kwargs)
