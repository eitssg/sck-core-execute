"""Delete a user from an AWS IAM account"""

from typing import Any
from botocore.exceptions import ClientError

import core_logging as log

from core_framework.models import DeploymentDetails, ActionDefinition, ActionParams

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


def generate_template() -> ActionDefinition:
    """Generate the action definition"""

    definition = ActionDefinition(
        Label="action-definition-label",
        Type="AWS::DeleteUser",
        DependsOn=["put-a-label-here"],
        Params=ActionParams(
            Account="The account to use for the action (required)",
            Region="The region to create the stack in (required)",
            UserName="The name of the user to delete (required)",
        ),
        Scope="Based on your deployment details, it one of 'portfolio', 'app', 'branch', or 'build'",
    )

    return definition


class DeleteUserAction(BaseAction):
    """Delete a user from an AWS IAM account

    This action will delete a user from an AWS IAM account.  The action will wait for the deletion to complete before returning.

    Attributes:
        Type: Use the value: ``AWS::DeleteUser``
        Params.Account: The account where the user is located
        Params.Region: The region where the user is located
        Params.UserName: The name of the user to delete (required)

    .. rubric: ActionDefinition:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Label: action-aws-deleteuser-label
              Type: "AWS::DeleteUser"
              Params:
                Account: "154798051514"
                UserName: "John Smith"
                Region: "ap-southeast-1"
              Scope: "build"

    """

    def __init__(
        self,
        definition: ActionDefinition,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

    def _execute(self):

        log.trace("DeleteUserAction._execute()")

        # Obtain an IAM client
        iam_client = aws.iam_client(
            region=self.params.Region,
            role=util.get_provisioning_role_arn(self.params.Account),
        )

        try:
            iam_client.get_user(UserName=self.params.UserName)
        except ClientError as e:
            if "NoSuchEntity" in e.response["Error"]["Code"]:
                log.warning("User '{}' does not exist", self.params.UserName)
                self.set_complete("User does not exist")
                return
            else:
                log.error("Error getting user '{}': {}", self.params.UserName, e)
                raise

        # list and delete signing certificates
        response = iam_client.list_signing_certificates(UserName=self.params.UserName)
        for certificate in response["Certificates"]:
            log.debug("Deleting signing certificate '{}'", certificate["CertificateId"])
            iam_client.delete_signing_certificate(
                UserName=self.params.UserName,
                CertificateId=certificate["CertificateId"],
            )

        # list and remove groups
        response = iam_client.list_groups_for_user(UserName=self.params.UserName)
        for group in response["Groups"]:
            log.debug("Removing group '{}'", group["GroupName"])
            iam_client.remove_user_from_group(
                UserName=self.params.UserName, GroupName=group["GroupName"]
            )

        # list and delete user policies
        response = iam_client.list_user_policies(UserName=self.params.UserName)
        for policy_name in response["PolicyNames"]:
            log.debug("Deleting policy '{}'", policy_name)
            iam_client.delete_user_policy(
                UserName=self.params.UserName, PolicyName=policy_name
            )

        # list and detach user policies
        response = iam_client.list_attached_user_policies(UserName=self.params.UserName)
        for policy in response["AttachedPolicies"]:
            log.debug("Detaching user policy '{}'", policy["PolicyArn"])
            iam_client.detach_user_policy(
                UserName=self.params.UserName, PolicyArn=policy["PolicyArn"]
            )

        # list and delete access keys
        response = iam_client.list_access_keys(UserName=self.params.UserName)
        for access_key in response["AccessKeyMetadata"]:
            log.debug("Deleting access key '{}'", access_key["AccessKeyId"])
            iam_client.delete_access_key(
                UserName=self.params.UserName, AccessKeyId=access_key["AccessKeyId"]
            )

        # Delete the user
        log.debug("Deleting user '{}'", self.params.UserName)

        iam_client.delete_user(UserName=self.params.UserName)

        self.set_complete()

        log.trace("DeleteUserAction._execute() complete")

    def _check(self):

        log.trace("DeleteUserAction._check()")

        self.set_complete()

        log.trace("DeleteUserAction._check() complete")

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):

        log.trace("DeleteUserAction._resolve()")

        self.params.Account = self.renderer.render_string(
            self.params.Account, self.context
        )
        self.params.Region = self.renderer.render_string(
            self.params.Region, self.context
        )
        self.params.UserName = self.renderer.render_string(
            self.params.UserName, self.context
        )

        log.trace("DeleteUserAction._resolve() complete")
