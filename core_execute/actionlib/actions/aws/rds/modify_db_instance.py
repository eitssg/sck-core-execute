"""Modify and RDS databae instance"""

from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator

from botocore.exceptions import ClientError

import core_helper.aws as aws

from core_framework.models import ActionSpec, DeploymentDetails

import core_framework as util
from core_execute.actionlib.action import BaseAction


class ModifyDbInstanceActionParams(BaseModel):
    """Parameters for the ModifyDbInstanceAction"""

    model_config = ConfigDict(populate_by_name=True, validate_assignment=True)

    account: str = Field(
        ...,
        alias="Account",
        description="The account to use for the action (required)",
    )
    region: str = Field(
        ...,
        alias="Region",
        description="The region to create the stack in (required)",
    )
    api_params: dict[str, Any] = Field(
        ...,
        alias="ApiParams",
        description="The parameters to pass to the modify_db_instance call (required). See AWS documentation for more information on the parameters.",
    )


class ModifyDbInstanceActionSpec(ActionSpec):
    """Generate the action definition"""

    @model_validator(mode="before")
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Validate the parameters for the ModifyDbInstanceActionSpec"""
        if not (values.get("label") or values.get("Label")):
            values["label"] = "action-aws-rds-modifydbinstance-label"
        if not (values.get("type") or values.get("Type")):
            values["type"] = "AWS::RDS::ModifyDbInstance"
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
    """Modify an RDS database instance

    This action modifies an RDS database instance.  The action will wait for the modifications to complete before returning.

    Attributes:
        Type: Use the value: ``AWS::RDS::ModifyDbInstance``
        Params.Account: The account where your RDS instance is located
        Params.Region: The region where your RDS instance is located
        Params.ApiParams: The parameters to pass to the modify_db_instance call (required).  See AWS docuemntation for more infomration on the parameters.

    .. rubric: ActionSpec:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Label: action-aws-rds-modifydbinstance-label
              Type: "AWS::RDS::ModifyDbInstance"
              Params:
                Account: "123456789012"
                Region: "ap-southeast-1"
                ApiParams:
                  DBInstanceIdentifier: "My-RDS-Instance-Id"
                  DBInstanceClass: db.m7g.4xlarge
                  ApplyImmediately: True
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
        self.params = ModifyDbInstanceActionParams(**definition.params)

    def _execute(self):
        # Obtain an RDS client
        rds_client = aws.rds_client(
            region=self.params.region,
            role=util.get_provisioning_role_arn(self.params.account),
        )

        self.set_running("Modifying DB instance")

        try:
            response = rds_client.modify_db_instance(**self.params.api_params)

            pending_modified_values = response["DBInstance"].get(
                "PendingModifiedValues", {}
            )
            if len(pending_modified_values) == 0:
                self.set_complete("All modifications complete")
            else:
                self.set_running(
                    "Waiting for modifications to complete: {}".format(
                        pending_modified_values
                    )
                )
        except ClientError as e:
            if "No modifications" in e.response["Error"]["Message"]:
                self.set_complete("No modifications to make")
            else:
                raise

    def _check(self):
        rds_client = aws.rds_client(
            region=self.params.region,
            role=util.get_provisioning_role_arn(self.params.account),
        )

        response = rds_client.describe_db_instances(
            DBInstanceIdentifier=self.params.api_params["DBInstanceIdentifier"]
        )

        pending_modified_values = response["DBInstances"][0].get(
            "PendingModifiedValues", {}
        )
        if len(pending_modified_values) == 0:
            self.set_complete("All modifications complete")
        else:
            self.set_running(
                "Waiting for modifications to complete: {}".format(
                    pending_modified_values
                )
            )

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):
        self.params.account = self.renderer.render_string(
            self.params.account, self.context
        )
        self.params.region = self.renderer.render_string(
            self.params.region, self.context
        )
        self.params.api_params = self.renderer.render_object(
            self.params.api_params, self.context
        )
