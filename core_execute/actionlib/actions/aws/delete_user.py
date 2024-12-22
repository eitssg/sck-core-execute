from typing import Any
from botocore.exceptions import ClientError

import core_logging as log

from core_framework.models import DeploymentDetails, ActionDefinition

import core_helper.aws as aws

import core_execute.envinfo as envinfo
from core_execute.actionlib.action import BaseAction


class DeleteUserAction(BaseAction):

    def __init__(
        self,
        definition: ActionDefinition,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        self.account = self.params.Account
        self.region = self.params.Region
        self.user_name = self.params.UserName

    def _execute(self):
        # Obtain an IAM client
        iam_client = aws.iam_client(
            region=self.region, role=envinfo.provisioning_role_arn(self.account)
        )

        try:
            iam_client.get_user(UserName=self.user_name)
        except ClientError as e:
            if "NoSuchEntity" in e.response["Error"]["Code"]:
                self.set_complete("User does not exist")
                return
            else:
                raise

        # list and delete signing certificates
        response = iam_client.list_signing_certificates(UserName=self.user_name)
        for certificate in response["Certificates"]:
            log.debug("Deleting signing certificate '{}'", certificate["CertificateId"])
            iam_client.delete_signing_certificate(
                UserName=self.user_name, CertificateId=certificate["CertificateId"]
            )

        # list and remove groups
        response = iam_client.list_groups_for_user(UserName=self.user_name)
        for group in response["Groups"]:
            log.debug("Removing group '{}'", group["GroupName"])
            iam_client.remove_user_from_group(
                UserName=self.user_name, GroupName=group["GroupName"]
            )

        # list and delete user policies
        response = iam_client.list_user_policies(UserName=self.user_name)
        for policy_name in response["PolicyNames"]:
            log.debug("Deleting policy '{}'", policy_name)
            iam_client.delete_user_policy(
                UserName=self.user_name, PolicyName=policy_name
            )

        # list and detach user policies
        response = iam_client.list_attached_user_policies(UserName=self.user_name)
        for policy in response["AttachedPolicies"]:
            log.debug("Detaching user policy '{}'", policy["PolicyArn"])
            iam_client.detach_user_policy(
                UserName=self.user_name, PolicyArn=policy["PolicyArn"]
            )

        # list and delete access keys
        response = iam_client.list_access_keys(UserName=self.user_name)
        for access_key in response["AccessKeyMetadata"]:
            log.debug("Deleting access key '{}'", access_key["AccessKeyId"])
            iam_client.delete_access_key(
                UserName=self.user_name, AccessKeyId=access_key["AccessKeyId"]
            )

        # Delete the user
        log.debug("Deleting user '{}'", self.user_name)
        iam_client.delete_user(UserName=self.user_name)

        self.set_complete()

    def _check(self):
        self.set_complete()

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):
        self.account = self.renderer.render_string(self.account, self.context)
        self.region = self.renderer.render_string(self.region, self.context)
        self.user_name = self.renderer.render_string(self.user_name, self.context)
