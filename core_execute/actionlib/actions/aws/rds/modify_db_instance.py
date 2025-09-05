"""Modify an Amazon RDS DB instance and track completion.

This action calls modify_db_instance on the RDS client, updates state/outputs with
the response, and continues checking until no PendingModifiedValues remain.
"""

from typing import Any
from pydantic import Field, model_validator

from botocore.exceptions import ClientError

import core_logging as log
import core_helper.aws as aws
import core_framework as util

from core_framework.models import ActionResource, ActionSpec, DeploymentDetails
from core_execute.actionlib.action import BaseAction


class ModifyDbInstanceActionSpec(ActionSpec):
    """Parameters for modifying an RDS DB instance.

    Attributes:
      account: AWS account ID used to assume the provisioning role.
      region: AWS region of the target DB instance.
      api_params: Arguments passed to boto3 RDS modify_db_instance.
    """

    api_params: dict[str, Any] = Field(
        ...,
        alias="ApiParams",
        description=("Parameters to pass to modify_db_instance (required). " "Refer to AWS docs for supported options."),
    )


class ModifyDbInstanceActionResource(ActionResource):
    """Resource definition and defaults for 'AWS::RDS::ModifyDbInstance'."""

    @model_validator(mode="before")
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Normalize/seed defaults for resource name/kind/scope/spec."""
        if not (values.get("name") or values.get("Name")):
            values["name"] = "action-aws-rds-modifydbinstance-name"
        if not (values.get("kind") or values.get("Kind")):
            values["kind"] = "AWS::RDS::ModifyDbInstance"
        if not values.get("depends_on", values.get("DependsOn")):  # arrays are falsy if empty
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Spec")):
            values["params"] = {
                "Account": "",
                "Region": "",
                "ApiParams": {},
            }
        return values


class ModifyDbInstanceAction(BaseAction):
    """Modify an RDS DB instance and wait until changes are applied.

    Calls modify_db_instance, records response metadata, and marks complete
    when PendingModifiedValues is empty; otherwise continues checking.
    """

    def __init__(
        self,
        definition: ActionResource,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
        parent_action_name: str | None = None,
    ):
        super().__init__(definition, context, deployment_details, parent_action_name)

        # Validate and load action parameters
        self.params = ModifyDbInstanceActionSpec(**definition.spec)

    def _execute(self):
        """Invoke modify_db_instance and record initial results.

        Sets:
          - Outputs: ModifiedInstance, AppliedApiParams, ResponseMetadata, PendingModifiedValues (if any)
          - Status: running if changes pending; complete if none

        Raises:
          ClientError: If the API call fails for non-trivial reasons.
        """
        # Obtain an RDS client
        rds_client = aws.rds_client(
            region=self.params.region,
            role=util.get_provisioning_role_arn(self.params.account),
        )

        self.set_running("Modifying DB instance")

        try:
            response = rds_client.modify_db_instance(**self.params.api_params)
            db_instance = response.get("DBInstance", {})
            pending_modified_values = db_instance.get("PendingModifiedValues", {})

            # Store state information about the modifications
            self.set_output("ModifiedInstance", db_instance)
            self.set_output("AppliedApiParams", self.params.api_params)
            self.set_output("ResponseMetadata", response.get("ResponseMetadata", {}))

            if not pending_modified_values:
                self.set_complete("All modifications complete")
            else:
                self.set_output("PendingModifiedValues", pending_modified_values)
                self.set_running(f"Waiting for modifications to complete: {pending_modified_values}")
        except ClientError as e:
            error_message = e.response.get("Error", {}).get("Message", "")
            if "No modifications" in error_message:
                self.set_complete("No modifications to make")
            else:
                log.error("Error during modify_db_instance: {}", e)
                raise

    def _check(self):
        """Poll describe_db_instances to determine if changes have completed.

        Marks complete when PendingModifiedValues is empty; otherwise remains running.

        Raises:
          ClientError: If describe_db_instances fails.
        """
        rds_client = aws.rds_client(
            region=self.params.region,
            role=util.get_provisioning_role_arn(self.params.account),
        )

        response = rds_client.describe_db_instances(DBInstanceIdentifier=self.params.api_params["DBInstanceIdentifier"])
        db_instance = response["DBInstances"][0]
        pending_modified_values = db_instance.get("PendingModifiedValues", {})

        if not pending_modified_values:
            self.set_complete("All modifications complete")
        else:
            self.set_running(f"Waiting for modifications to complete: {pending_modified_values}")

    def _unexecute(self):
        """No rollback support; reversing RDS modifications is not supported."""
        self.set_complete("Unexecute not supported for RDS modifications")

    def _cancel(self):
        """No-op; RDS modifications cannot be cancelled mid-flight."""
        self.set_complete("Cancel not supported for RDS modifications")

    def _resolve(self):
        """Render templates for account, region, and api_params."""
        self.params.account = self.renderer.render_string(self.params.account, self.context)
        self.params.region = self.renderer.render_string(self.params.region, self.context)
        self.params.api_params = self.renderer.render_object(self.params.api_params, self.context)

    @classmethod
    def generate_action_resource(cls, **kwargs) -> ModifyDbInstanceActionResource:
        """Factory: create a typed ModifyDbInstanceActionResource."""
        return ModifyDbInstanceActionResource(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> ModifyDbInstanceActionSpec:
        """Factory: create typed ModifyDbInstanceActionSpec."""
        return ModifyDbInstanceActionSpec(**kwargs)
