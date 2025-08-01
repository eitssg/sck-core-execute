"""Modify an RDS database instance.

This module defines the ModifyDbInstanceAction which is responsible for calling
the boto3 RDS client's modify_db_instance() method to make modifications to an RDS
instance. The action waits until the modifications are completed as indicated by
the RDS API before setting its state to complete.

:Example:

   .. code-block:: yaml

      - Name: action-aws-rds-modifydbinstance-name
        Kind: "AWS::RDS::ModifyDbInstance"
        Params:
          Account: "123456789012"
          Region: "ap-southeast-1"
          ApiParams:
            DBInstanceIdentifier: "My-RDS-Instance-Id"
            DBInstanceClass: db.m7g.4xlarge
            ApplyImmediately: True
        Scope: "build"
"""

from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator

from botocore.exceptions import ClientError

import core_logging as log
import core_helper.aws as aws
import core_framework as util

from core_framework.models import ActionSpec, ActionParams, DeploymentDetails
from core_execute.actionlib.action import BaseAction


class ModifyDbInstanceActionParams(ActionParams):
    """Parameters for the ModifyDbInstanceAction.

    :param account: The account to use for the action.
    :type account: str
    :param region: The AWS region of the RDS instance.
    :type region: str
    :param api_params: The parameters to pass to the modify_db_instance API call.
           See AWS documentation for available options.
    :type api_params: dict[str, Any]
    """

    api_params: dict[str, Any] = Field(
        ...,
        alias="ApiParams",
        description=(
            "The parameters to pass to the modify_db_instance call (required). "
            "Refer to the AWS documentation for supported options."
        ),
    )


class ModifyDbInstanceActionSpec(ActionSpec):
    """Generate the action definition for modifying an RDS database instance.

    This specification validates and sets defaults for the action parameters.
    """

    @model_validator(mode="before")
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Validate the parameters for the ModifyDbInstanceActionSpec.

        :param values: Incoming parameter values.
        :type values: dict[str, Any]
        :return: The validated values.
        :rtype: dict[str, Any]
        """
        if not (values.get("name") or values.get("Name")):
            values["name"] = "action-aws-rds-modifydbinstance-name"
        if not (values.get("kind") or values.get("Kind")):
            values["kind"] = "AWS::RDS::ModifyDbInstance"
        if not (values.get("depends_on") or values.get("DependsOn")):
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Params")):
            values["params"] = {
                "Account": "",
                "Region": "",
                "ApiParams": {},
            }
        return values


class ModifyDbInstanceAction(BaseAction):
    """Modify an RDS database instance.

    This action modifies an RDS database instance by calling the boto3 RDS client's
    modify_db_instance() method. It then checks whether the modifications are complete
    based on the 'PendingModifiedValues' returned in the response.

    :param definition: The action specification containing parameters.
    :type definition: ActionSpec
    :param context: The execution context used for template rendering.
    :type context: dict[str, Any]
    :param deployment_details: The deployment details for the action.
    :type deployment_details: DeploymentDetails
    """

    def __init__(
        self,
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)
        # Validate and load action parameters
        self.params = ModifyDbInstanceActionParams(**definition.params)

    def _execute(self):
        """
        Execute the RDS modify_db_instance operation.

        This method obtains an RDS client using the provided region and account's provisioning role.
        It calls the modify_db_instance() API with the provided ApiParams. If there are no pending
        modifications reported in the response, the action is marked complete. Otherwise, it continues
        running until modifications complete, storing state information about the change.

        :raises ClientError: If there is an error calling the modify_db_instance API.
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
                self.set_running("Waiting for modifications to complete: {}".format(pending_modified_values))
        except ClientError as e:
            error_message = e.response.get("Error", {}).get("Message", "")
            if "No modifications" in error_message:
                self.set_complete("No modifications to make")
            else:
                log.error("Error during modify_db_instance: {}", e)
                raise

    def _check(self):
        """
        Check the status of the modifications on the RDS instance.

        This method calls describe_db_instances() with the DBInstanceIdentifier and
        checks if there are any pending modified values. If not, the action is marked complete.

        :raises ClientError: If there is an error calling the describe_db_instances() API.
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
            self.set_running("Waiting for modifications to complete: {}".format(pending_modified_values))

    def _unexecute(self):
        """
        Reverse the modifications to the RDS instance if possible.

        Note:
            Reversing modifications to an RDS instance is generally not supported.
            This method is implemented as a placeholder.
        """
        self.set_complete("Unexecute not supported for RDS modifications")

    def _cancel(self):
        """
        Cancel the RDS modify action.

        Note:
            Cancelling the modification process is not implemented as RDS modifications
            are typically in progress and cannot be "cancelled" midway.
        """
        self.set_complete("Cancel not supported for RDS modifications")

    def _resolve(self):
        """
        Resolve template variables in the action parameters.

        This method uses the Jinja2 renderer to resolve any template strings or objects in the
        account, region, and api_params fields.
        """
        self.params.account = self.renderer.render_string(self.params.account, self.context)
        self.params.region = self.renderer.render_string(self.params.region, self.context)
        self.params.api_params = self.renderer.render_object(self.params.api_params, self.context)

    @classmethod
    def generate_action_spec(cls, **kwargs) -> ModifyDbInstanceActionSpec:
        return ModifyDbInstanceActionSpec(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> ModifyDbInstanceActionParams:
        return ModifyDbInstanceActionParams(**kwargs)
