"""Delete a user from an AWS IAM account"""

from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator

from botocore.exceptions import ClientError

import core_logging as log

from core_framework.models import DeploymentDetails, ActionSpec

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


class DeleteStackActionParams(BaseModel):
    """Parameters for the DeleteStackAction"""

    model_config = ConfigDict(populate_by_name=True, validate_assignment=True)

    account: str = Field(
        ..., alias="Account", description="The account to use for the action (required)"
    )
    region: str = Field(
        ..., alias="Region", description="The region to create the stack in (required)"
    )
    user_name: str = Field(
        ...,
        alias="UserName",
        description="The name of the user to delete (required)",
    )


class DeleteUserActionSpec(ActionSpec):
    """Generate the action definition"""

    @model_validator(mode="before")
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Validate the parameters for the DeleteUserActionSpec"""
        if not (values.get("label") or values.get("Label")):
            values["label"] = "action-aws-deleteuser-label"
        if not (values.get("type") or values.get("Type")):
            values["type"] = "AWS::DeleteUser"
        if not (values.get("depends_on") or values.get("DependsOn")):
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Params")):
            values["params"] = {
                "account": "",
                "region": "",
                "user_name": "",
            }
        return values


class DeleteUserAction(BaseAction):
    """Delete a user from an AWS IAM account

    This action will delete a user from an AWS IAM account.  The action will wait for the deletion to complete before returning.

    Attributes:
        Type: Use the value: ``AWS::DeleteUser``
        Params.Account: The account where the user is located
        Params.Region: The region where the user is located
        Params.UserName: The name of the user to delete (required)

    .. rubric: ActionSpec:

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
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        # validate the parameters
        self.params = DeleteStackActionParams.model_validate(**definition.params)

    def _execute(self):

        log.trace("DeleteUserAction._execute()")

        # Obtain an IAM client
        iam_client = aws.iam_client(
            region=self.params.region,
            role=util.get_provisioning_role_arn(self.params.account),
        )

        try:
            iam_client.get_user(UserName=self.params.user_name)
        except ClientError as e:
            if "NoSuchEntity" in e.response["Error"]["Code"]:
                log.warning("User '{}' does not exist", self.params.user_name)
                self.set_complete("User does not exist")
                return
            else:
                log.error("Error getting user '{}': {}", self.params.user_name, e)
                raise

        # list and delete signing certificates
        response = iam_client.list_signing_certificates(UserName=self.params.user_name)
        for certificate in response["Certificates"]:
            log.debug("Deleting signing certificate '{}'", certificate["CertificateId"])
            iam_client.delete_signing_certificate(
                UserName=self.params.user_name,
                CertificateId=certificate["CertificateId"],
            )

        # list and remove groups
        response = iam_client.list_groups_for_user(UserName=self.params.user_name)
        for group in response["Groups"]:
            log.debug("Removing group '{}'", group["GroupName"])
            iam_client.remove_user_from_group(
                UserName=self.params.user_name, GroupName=group["GroupName"]
            )

        # list and delete user policies
        response = iam_client.list_user_policies(UserName=self.params.user_name)
        for policy_name in response["PolicyNames"]:
            log.debug("Deleting policy '{}'", policy_name)
            iam_client.delete_user_policy(
                UserName=self.params.user_name, PolicyName=policy_name
            )

        # list and detach user policies
        response = iam_client.list_attached_user_policies(
            UserName=self.params.user_name
        )
        for policy in response["AttachedPolicies"]:
            log.debug("Detaching user policy '{}'", policy["PolicyArn"])
            iam_client.detach_user_policy(
                UserName=self.params.user_name, PolicyArn=policy["PolicyArn"]
            )

        # list and delete access keys
        response = iam_client.list_access_keys(UserName=self.params.user_name)
        for access_key in response["AccessKeyMetadata"]:
            log.debug("Deleting access key '{}'", access_key["AccessKeyId"])
            iam_client.delete_access_key(
                UserName=self.params.user_name, AccessKeyId=access_key["AccessKeyId"]
            )

        # Delete the user
        log.debug("Deleting user '{}'", self.params.user_name)

        iam_client.delete_user(UserName=self.params.user_name)

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

        self.params.account = self.renderer.render_string(
            self.params.account, self.context
        )
        self.params.region = self.renderer.render_string(
            self.params.region, self.context
        )
        self.params.user_name = self.renderer.render_string(
            self.params.user_name, self.context
        )

        log.trace("DeleteUserAction._resolve() complete")
