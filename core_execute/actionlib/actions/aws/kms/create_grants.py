"""Grant access to KMS keys to principals"""

from typing import Any

import core_logging as log

import core_helper.aws as aws

from core_framework.models import DeploymentDetails, ActionDefinition, ActionParams

import core_framework as util
from core_execute.actionlib.action import BaseAction

import re


def generate_template() -> ActionDefinition:
    """Generate the action definition"""

    definition = ActionDefinition(
        Label="action-definition-label",
        Type="AWS::KMS::CreateGrants",
        DependsOn=["put-a-label-here"],
        Params=ActionParams(
            Account="The account to use for the action (required)",
            Region="The region to create the stack in (required)",
            KmsKeyId="The ID of the KMS key to create grants for (optionally required)",
            KmsKeyArn="The ARN of the KMS key to create grants for (optionally required)",
            GranteePrincipals=["The principals to grant access to (required)"],
            Operations=["The operations to grant access for (required)"],
            IgnoreFailedGrants=False,
        ),
        Scope="Based on your deployment details, it one of 'portfolio', 'app', 'branch', or 'build'",
    )

    return definition


class CreateGrantsAction(BaseAction):
    """Create Grans for an AWS KMS Key

    This action will create grants for KMS Keys.  The action will wait for the modifications to complete before returning.

    Attributes:
        Label: Enter a label to define this action instance
        Type: Use the value: ``AWS::KMS::CreateGrants``
        Params.Account: The accoutn where KMS keys are centraly stored
        Params.Region: The region where KMS keys are located
        Params.KmsKeyArn: The ID of the KMS key to create grants for (required if KmsKeyId is not provided)
        Params.KmsKeyId: The ARN of the KMS key to create grants for (required if KmsKeyArn is not provided)
        Params.GranteePrincipals: The principals to grant access to (required)
        Params.Operations: The operations to grant access for (required)
        Params.IgnoreFailedGrants: If true, ignore failed grants, otherwise fail the action if a grant fails

    .. rubric: ActionDefinition:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Label: action-aws-kms-creategrants-label
              Type: "AWS::KMS::CreateGrants"
              Params:
                Account: "123456789012"
                Region: "ap-southeast-1"
                KmsKeyArn: "arn:aws:kms:ap-southeast-1:123456789012:key/your-kms-key-id"
                GrantPrincipals: ["arn:aws:iam::123456789012:role/YourRole"]
                Operations: ["Encrypt", "Decrypt", "GenerateDataKey"]
                IgnoreFailedGrants: false
              Scope: "build"

    """

    def __init__(
        self,
        definition: ActionDefinition,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        self.account = self.params.Account
        self.region = self.params.Region
        self.kms_key_id = self.params.KmsKeyId or self.params.KmsKeyArn
        self.grantee_principals = self.params.GranteePrincipals
        self.operations = self.params.Operations
        self.ignore_failed_grants = (
            self.params.IgnoreFailedGrants if self.params.IgnoreFailedGrants else True
        )

    def _execute(self):
        # Obtain an EC2 client
        kms_client = aws.kms_client(
            region=self.region, role=util.get_provisioning_role_arn(self.account)
        )

        # Create the grants
        self.set_running(
            "Creating grants for KMS key '{}' in account '{}'".format(
                self.kms_key_id, self.account
            )
        )
        for principal in self.grantee_principals:
            # Use the grantee principal as the grant name, with unsupported characters replaced with '-'
            name = re.sub(r"[^a-zA-Z0-9:/_-]", "-", principal)

            try:
                params = {
                    "GranteePrincipal": principal,
                    "KeyId": self.kms_key_id,
                    "Name": name,
                    "Operations": self.operations,
                }

                log.debug("Creating grant", details=params)

                response = kms_client.create_grant(**params)

                log.debug(
                    "Grant creation was successful",
                    details={
                        "GrantId": response["GrantId"],
                        "GrantToken": response["GrantToken"],
                    },
                )
            except Exception as e:
                if self.ignore_failed_grants:
                    log.trace(
                        "Failed to create grant, but configured to ignore - {}", e
                    )
                else:
                    log.error("Failed to create grant - {}", e)
                    self.set_failed("Failed to create grant, principal may not exist")

        self.set_complete()

    def _check(self):
        self.set_failed("Internal error - _check() should not have been called")

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):
        self.account = self.renderer.render_string(self.account, self.context)
        self.region = self.renderer.render_string(self.region, self.context)
        self.kms_key_id = self.renderer.render_string(self.kms_key_id, self.context)
        self.grantee_principals = self.renderer.render_object(
            self.grantee_principals, self.context
        )
        self.ignore_failed_grants = self.renderer.render_string(
            self.ignore_failed_grants, self.context
        )
