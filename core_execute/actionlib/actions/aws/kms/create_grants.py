"""Create AWS KMS grants for one or more principals.

This action creates KMS grants on a given key for specified principals and operations,
optionally ignoring failures per-principal. Results are written to state/outputs.
"""

from typing import Any
from pydantic import model_validator, Field, field_validator
import core_logging as log

import core_helper.aws as aws

from core_framework.models import DeploymentDetails, ActionResource, ActionSpec

import core_framework as util
from core_execute.actionlib.action import BaseAction

import re


class CreateGrantsActionResource(ActionResource):
    """Resource definition for the KMS grant creation action."""

    @model_validator(mode="before")
    def validate_params(cls, values) -> dict:
        """Seed default resource fields and ensure required structure.

        Args:
          values: Raw resource dict.

        Returns:
          Normalized resource dict with name/kind/scope/params defaults.
        """

        if "Kind" in values:
            del values["Kind"]

        values["kind"] = "AWS::KMS::CreateGrants"

        return values


class CreateGrantsActionSpec(ActionSpec):
    """Parameters for creating KMS grants.

    Attributes:
      kms_key_id: KMS key ID (optional if kms_key_arn provided).
      kms_key_arn: KMS key ARN (optional if kms_key_id provided).
      grantee_principals: Principals to grant access to.
      operations: KMS operations to allow for each grant.
      ignore_failed_grants: If 'true', continue even if some grants fail (templated string).
    """

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
    ignore_failed_grants: str = Field(
        "false",  # String default
        alias="IgnoreFailedGrants",
        description="If 'true', ignore failed grants; otherwise fail on first error. May contain Jinja2.",
    )

    @model_validator(mode="after")
    def validate_kms_key(self):
        """Ensure either kms_key_id or kms_key_arn is provided."""
        if not self.kms_key_id and not self.kms_key_arn:
            raise ValueError("Either kms_key_id or kms_key_arn must be provided")
        return self

    @field_validator("grantee_principals")
    @classmethod
    def validate_grantee_principals(cls, v):
        """Validate grantee_principals is not empty."""
        if not v:
            raise ValueError("grantee_principals cannot be empty")
        return v

    @field_validator("operations")
    @classmethod
    def validate_operations(cls, v):
        """Validate operations is not empty and contains valid KMS operations."""
        if not v:
            raise ValueError("operations cannot be empty")

        valid_operations = {
            "Encrypt",
            "Decrypt",
            "GenerateDataKey",
            "GenerateDataKeyWithoutPlaintext",
            "ReEncryptFrom",
            "ReEncryptTo",
            "CreateGrant",
            "RetireGrant",
            "DescribeKey",
            "GenerateDataKeyPair",
            "GenerateDataKeyPairWithoutPlaintext",
            "GetPublicKey",
            "Sign",
            "Verify",
        }

        invalid_ops = set(v) - valid_operations
        if invalid_ops:
            raise ValueError(f"Invalid operations: {invalid_ops}. Valid operations: {valid_operations}")

        return v


class CreateGrantsAction(BaseAction[CreateGrantsActionSpec]):
    """Create KMS grants for a key and optionally ignore per-principal failures.

    Writes summary and per-grant details to outputs, including GrantIds, GrantTokens,
    and principal mappings. Can retire created grants on unexecute.
    """

    def __init__(
        self,
        definition: ActionResource,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        """Initialize the KMS grant action and load parameters.

        Args:
          definition: Action resource definition.
          context: Template rendering context.
          deployment_details: Deployment metadata for this run.
        """
        super().__init__(definition, context, deployment_details)

        self.spec = CreateGrantsActionSpec(**definition.spec)

        self.name = definition.name
        self.account = self.spec.account
        self.region = self.spec.region
        self.kms_key_id = self.spec.kms_key_id or self.spec.kms_key_arn
        self.grantee_principals = self.spec.grantee_principals
        self.operations = self.spec.operations
        self.ignore_failed_grants = self.spec.ignore_failed_grants

    def _execute(self):
        """Create grants for each principal and record results.

        Behavior:
          - Creates a grant per principal using the provided operations.
          - If ignore_failed_grants is truthy, continues on failures and records them.
          - Sets outputs: CreatedGrants, GrantIds, GrantTokens, summary counts, etc.
        """
        try:
            # Convert ignore_failed_grants string to boolean
            ignore_failures = self._string_to_bool(self.ignore_failed_grants)

            # Obtain a KMS client
            kms_client = aws.kms_client(region=self.region, role_arn=util.get_provisioning_role_arn(self.account))

            # Create the grants
            self.set_running("Creating grants for KMS key '{}' in account '{}'".format(self.kms_key_id, self.account))

            successful_grants = 0
            failed_grants = 0
            created_grants = []
            failed_principals = []

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

                    grant_info = {
                        "GrantId": response["GrantId"],
                        "GrantToken": response["GrantToken"],
                        "Principal": principal,
                        "GrantName": name,
                        "Operations": list(self.operations),
                        "KeyId": self.kms_key_id,
                        "CreatedAt": util.get_current_timestamp(),  # Add timestamp
                    }

                    created_grants.append(grant_info)

                    log.debug(
                        "Grant creation was successful",
                        details=grant_info,
                    )
                    successful_grants += 1

                except Exception as e:
                    failed_grants += 1
                    failure_info = {
                        "Principal": principal,
                        "GrantName": name,
                        "Error": str(e),
                        "ErrorType": type(e).__name__,
                        "FailedAt": util.get_current_timestamp(),
                    }
                    failed_principals.append(failure_info)

                    if ignore_failures:
                        log.warning(
                            "Failed to create grant for principal '{}', but configured to ignore - {}",
                            principal,
                            e,
                        )
                    else:
                        log.error(
                            "Failed to create grant for principal '{}' - {}",
                            principal,
                            e,
                        )
                        # Store partial results even on failure
                        self._store_grant_state(
                            created_grants,
                            failed_principals,
                            successful_grants,
                            failed_grants,
                        )
                        self.set_failed(f"Failed to create grant for principal '{principal}', principal may not exist")
                        return

            # Store all grant information in state
            self._store_grant_state(created_grants, failed_principals, successful_grants, failed_grants)

            log.info(
                "Grant creation completed: {} successful, {} failed (ignore_failed_grants={})",
                successful_grants,
                failed_grants,
                ignore_failures,
            )
            self.set_complete()

        except Exception as e:
            log.error("Error during grant creation: {}", e)
            self.set_failed(f"Error during grant creation: {str(e)}")

    def _store_grant_state(
        self,
        created_grants: list,
        failed_principals: list,
        successful_count: int,
        failed_count: int,
    ):
        """Persist grant creation results to outputs.

        Args:
          created_grants: Successfully created grants with metadata.
          failed_principals: Failures with principal and error details.
          successful_count: Number of successful grants.
          failed_count: Number of failed grants.
        """
        # Store summary information
        self.set_output("TotalGrantsCreated", successful_count)
        self.set_output("TotalGrantsFailed", failed_count)
        self.set_output("TotalPrincipals", len(self.grantee_principals))
        self.set_output("KmsKeyId", self.kms_key_id)
        self.set_output("Region", self.region)
        self.set_output("Account", self.account)

        # Store individual grant details
        if created_grants:
            self.set_output("CreatedGrants", created_grants)

            # Store individual grant IDs for easy reference
            grant_ids = [grant["GrantId"] for grant in created_grants]
            self.set_output("GrantIds", grant_ids)

            # Store grant tokens for immediate use
            grant_tokens = [grant["GrantToken"] for grant in created_grants]
            self.set_output("GrantTokens", grant_tokens)

            # Store principal mappings
            principal_to_grant = {grant["Principal"]: grant["GrantId"] for grant in created_grants}
            self.set_output("PrincipalToGrantMapping", principal_to_grant)

        # Store failure information if any
        if failed_principals:
            self.set_output("FailedPrincipals", failed_principals)
            failed_principal_names = [failure["Principal"] for failure in failed_principals]
            self.set_output("FailedPrincipalNames", failed_principal_names)

        # Store the operations granted
        self.set_output("GrantedOperations", list(self.operations))

        # Store execution metadata
        self.set_output("ExecutionTimestamp", util.get_current_timestamp())
        self.set_output("IgnoreFailedGrants", self.ignore_failed_grants)

    def _check(self):
        """Verify created grants still exist and mark completion."""
        try:
            # Get the stored grant information
            created_grants = self.get_output("CreatedGrants", [])

            if not created_grants:
                log.debug("No grants to check for action '{}'", self.name)
                self.set_complete()
                return

            # Obtain a KMS client
            kms_client = aws.kms_client(region=self.region, role_arn=util.get_provisioning_role_arn(self.account))

            self.set_running(f"Checking status of {len(created_grants)} grants")

            # Get all grants for the key ONCE
            try:
                response = kms_client.list_grants(KeyId=self.kms_key_id)
                existing_grant_ids = {g["GrantId"] for g in response.get("Grants", [])}
            except Exception as e:
                log.error("Error listing grants for key {}: {}", self.kms_key_id, e)
                self.set_failed(f"Error listing grants: {str(e)}")
                return

            active_grants = []
            inactive_grants = []

            # Now check each grant against the single API response
            for grant in created_grants:
                if grant["GrantId"] in existing_grant_ids:
                    active_grants.append(grant)
                    log.debug("Grant {} is active", grant["GrantId"])
                else:
                    inactive_grants.append(grant)
                    log.warning("Grant {} is no longer active", grant["GrantId"])

            # Update state with current status
            self.set_output("ActiveGrants", active_grants)
            self.set_output("InactiveGrants", inactive_grants)
            self.set_output("LastChecked", util.get_current_timestamp())

            if inactive_grants:
                log.warning(
                    "Found {} inactive grants out of {} total",
                    len(inactive_grants),
                    len(created_grants),
                )

            self.set_complete()

        except Exception as e:
            log.error("Error checking grant status: {}", e)
            self.set_failed(f"Error checking grant status: {str(e)}")

    def _unexecute(self):
        """Retire all grants created by this action (best-effort)."""
        try:
            # Get the stored grant information
            created_grants = self.get_output("CreatedGrants", [])

            if not created_grants:
                log.debug("No grants to retire for action '{}'", self.name)
                self.set_complete()
                return

            # Obtain a KMS client
            kms_client = aws.kms_client(region=self.region, role_arn=util.get_provisioning_role_arn(self.account))

            self.set_running(f"Retiring {len(created_grants)} grants")

            retired_grants = []
            failed_retirements = []

            for grant in created_grants:
                try:
                    # Retire the grant
                    kms_client.retire_grant(KeyId=self.kms_key_id, GrantId=grant["GrantId"])

                    retired_grants.append(grant)
                    log.debug("Successfully retired grant {}", grant["GrantId"])

                except Exception as e:
                    failed_retirements.append(
                        {
                            "Grant": grant,
                            "Error": str(e),
                            "ErrorType": type(e).__name__,
                        }
                    )
                    log.error("Failed to retire grant {}: {}", grant["GrantId"], e)

            # Update state with retirement results
            self.set_output("RetiredGrants", retired_grants)
            self.set_output("FailedRetirements", failed_retirements)
            self.set_output("RetirementTimestamp", util.get_current_timestamp())

            if failed_retirements:
                log.warning(
                    "Failed to retire {} grants out of {} total",
                    len(failed_retirements),
                    len(created_grants),
                )

            log.info(
                "Grant retirement completed: {} retired, {} failed",
                len(retired_grants),
                len(failed_retirements),
            )

            self.set_complete()

        except Exception as e:
            log.error("Error during grant retirement: {}", e)
            self.set_failed(f"Error during grant retirement: {str(e)}")

    def _string_to_bool(self, value: str) -> bool:
        """Return True for truthy strings like 'true', '1', 'yes', 'on'."""
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower().strip() in ("true", "1", "yes", "on")
        return False

    def _resolve(self):
        """Render templates for account, region, key ID/ARN, principals, operations, and flags."""
        try:
            self.account = self.renderer.render_string(self.account, self.context)
            self.region = self.renderer.render_string(self.region, self.context)
            self.kms_key_id = self.renderer.render_string(self.kms_key_id, self.context)
            self.grantee_principals = self.renderer.render_object(self.grantee_principals, self.context)
            self.operations = self.renderer.render_object(self.operations, self.context)
            # Render the ignore_failed_grants template
            self.ignore_failed_grants = self.renderer.render_string(self.ignore_failed_grants, self.context)

            log.debug("Resolved ignore_failed_grants to: '{}'", self.ignore_failed_grants)

        except Exception as e:
            log.error("Error resolving template variables: {}", e)
            self.set_failed(f"Error resolving template variables: {str(e)}")

    def _cancel(self):
        """No-op; KMS grant creation is fast and cannot be cancelled."""
        log.debug("Cancel requested for action '{}' - no action taken", self.name)
        self.set_complete()

    @classmethod
    def generate_action_resource(cls, **kwargs) -> CreateGrantsActionResource:
        """Factory: create a typed CreateGrantsActionResource."""
        return CreateGrantsActionResource(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> CreateGrantsActionSpec:
        """Factory: create typed CreateGrantsActionSpec."""
        return CreateGrantsActionSpec(**kwargs)
