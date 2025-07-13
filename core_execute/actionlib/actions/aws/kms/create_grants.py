"""Grant access to KMS keys to principals"""

from typing import Any
from pydantic import BaseModel, ConfigDict, model_validator, Field
import core_logging as log

import core_helper.aws as aws

from core_framework.models import DeploymentDetails, ActionSpec

import core_framework as util
from core_execute.actionlib.action import BaseAction

import re


class CreateGrantsActionSpec(ActionSpec):

    @model_validator(mode="before")
    def validate_params(cls, values) -> dict:
        """Validate the parameters for the CreateGrantsActionSpec"""
        if not (values.get("label") or values.get("Label")):
            values["label"] = "action-aws-kms-creategrants-label"
        if not (values.get("type") or values.get("Type")):
            values["type"] = "AWS::KMS::CreateGrants"
        if not (values.get("depends_on") or values.get("DependsOn")):
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Params")):
            values["params"] = {
                "account": "",
                "region": "",
                "kms_key_id": "",
                "grantee_principals": [],
                "operations": [],
                "ignore_failed_grants": False,
            }
        return values


class CreateGrantsActionParams(BaseModel):
    """Parameters for the CreateGrantsAction

    Attributes:
        Account="The account to use for the action (required)",
        Region="The region to create the stack in (required)",
        KmsKeyId="The ID of the KMS key to create grants for (optionally required)",
        KmsKeyArn="The ARN of the KMS key to create grants for (optionally required)",
        GranteePrincipals=["The principals to grant access to (required)"],
        Operations=["The operations to grant access for (required)"],
        IgnoreFailedGrants=False

    """

    model_config = ConfigDict(populate_by_name=True, validate_assignment=True)

    account: str = Field(
        ..., alias="Account", description="The account to use for the action (required)"
    )
    region: str = Field(
        ..., alias="Region", description="The region to create the stack in (required)"
    )
    kms_key_id: str | None = Field(
        None,
        alias="KmsKeyId",
        description="The ID of the KMS key to create grants for (optionally required)",
    )
    kms_key_arn: str | None = Field(
        None,
        alias="KmsKeyArn",
        description="The ARN of the KMS key to create grants for (optionally required)",
    )
    grantee_principals: list[str] = Field(
        ...,
        alias="GranteePrincipals",
        description="The principals to grant access to (required)",
    )
    operations: list[str] = Field(
        ...,
        alias="Operations",
        description="The operations to grant access for (required)",
    )
    ignore_failed_grants: bool = Field(
        False,
        alias="IgnoreFailedGrants",
        description="If true, ignore failed grants, otherwise fail the action if a grant fails",
    )


class CreateGrantsAction(BaseAction):
    """Create Grans for an AWS KMS Key

    This action will create grants for KMS Keys.  The action will wait for the modifications to complete before returning.

    Type: Use the value: ``AWS::KMS::CreateGrants``

    .. rubric: ActionSpec:

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
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        self.params = CreateGrantsActionParams(**definition.params)

        self.account = self.params.account
        self.region = self.params.region
        self.kms_key_id = self.params.kms_key_id or self.params.kms_key_arn
        self.grantee_principals = self.params.grantee_principals
        self.operations = self.params.operations
        self.ignore_failed_grants = (
            self.params.ignore_failed_grants
            if self.params.ignore_failed_grants
            else True
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
