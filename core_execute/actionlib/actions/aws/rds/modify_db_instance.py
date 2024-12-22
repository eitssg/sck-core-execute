from botocore.exceptions import ClientError

import core_helper.aws as aws

import core_execute.envinfo as envinfo
from core_execute.actionlib.action import BaseAction


class ModifyDbInstanceAction(BaseAction):
    def __init__(self, definition, context, deployment_details):
        super().__init__(definition, context, deployment_details)
        self.account = self.params["Account"]
        self.region = self.params["Region"]
        self.api_params = self.params["ApiParams"]

    def _execute(self):
        # Obtain an RDS client
        rds_client = aws.rds_client(
            region=self.region, role=envinfo.provisioning_role_arn(self.account)
        )

        self.set_running("Modifying DB instance")

        try:
            response = rds_client.modify_db_instance(**self.api_params)

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
            region=self.region, role=envinfo.provisioning_role_arn(self.account)
        )

        response = rds_client.describe_db_instances(
            DBInstanceIdentifier=self.api_params["DBInstanceIdentifier"]
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
        self.account = self.renderer.render_string(self.account, self.context)
        self.region = self.renderer.render_string(self.region, self.context)
        self.api_params = self.renderer.render_object(self.api_params, self.context)
