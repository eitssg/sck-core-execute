"""Modify and RDS databae instance"""

from typing import Any

from botocore.exceptions import ClientError

import core_helper.aws as aws

from core_framework.models import ActionDefinition, DeploymentDetails, ActionParams

import core_framework as util
from core_execute.actionlib.action import BaseAction


def generate_template() -> ActionDefinition:
    """Generate the action definition"""

    definition = ActionDefinition(
        Label="action-definition-label",
        Type="AWS::RDS::ModifyDbInstance",
        DependsOn=['put-a-label-here'],
        Params=ActionParams(
            Account="The account to use for the action (required)",
            Region="The region to create the stack in (required)",
            ApiParams={
                "any": "The parameters to pass to the modify_db_instance call (required)"
            },
        ),
        Scope="Based on your deployment details, it one of 'portfolio', 'app', 'branch', or 'build'",
    )

    return definition


class ModifyDbInstanceAction(BaseAction):
    def __init__(
        self,
        definition: ActionDefinition,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

    def _execute(self):
        # Obtain an RDS client
        rds_client = aws.rds_client(
            region=self.region, role=util.get_provisioning_role_arn(self.params.Account)
        )

        self.set_running("Modifying DB instance")

        try:
            response = rds_client.modify_db_instance(**self.params.ApiParams)

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
            region=self.params.Region, role=util.get_provisioning_role_arn(self.params.Account)
        )

        response = rds_client.describe_db_instances(
            DBInstanceIdentifier=self.params.ApiParams["DBInstanceIdentifier"]
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
        self.params.Account = self.renderer.render_string(self.params.Account, self.context)
        self.params.Region = self.renderer.render_string(self.params.Region, self.context)
        self.params.ApiParams = self.renderer.render_object(self.params.ApiParams, self.context)
