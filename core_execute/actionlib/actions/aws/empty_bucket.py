"""Empty and S3 bucket"""
from typing import Any

from botocore.exceptions import ClientError

import core_logging as log

from core_framework.models import DeploymentDetails, ActionDefinition, ActionParams

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


def generate_template() -> ActionDefinition:
    """Generate the action definition"""

    definition = ActionDefinition(
        Label="action-definition-label",
        Type="AWS::EmptyBucket",
        DependsOn=['put-a-label-here'],
        Params=ActionParams(
            Account="The account to use for the action (required)",
            Region="The region to create the stack in (required)",
            BucketName="The name of the bucket to empty (required)",
        ),
        Scope="Based on your deployment details, it one of 'portfolio', 'app', 'branch', or 'build'",
    )

    return definition


class EmptyBucketAction(BaseAction):
    def __init__(
        self,
        definition: ActionDefinition,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)
        self.account = self.params.Account
        self.region = self.params.Region
        self.bucket_name = self.params.BucketName

    def _execute(self):
        if self.bucket_name:
            self.set_running(
                "Deleting all objects in bucket '{}'".format(self.bucket_name)
            )
            self.__empty_bucket()
        else:
            self.set_complete("No bucket specified")

    def _check(self):
        self.__empty_bucket()

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):
        self.region = self.renderer.render_string(self.region, self.context)
        self.account = self.renderer.render_string(self.account, self.context)
        self.bucket_name = self.renderer.render_string(self.bucket_name, self.context)

    def __empty_bucket(self):
        # Obtain a CloudFormation client
        s3_resource = aws.s3_resource(
            region=self.region, role=util.get_provisioning_role_arn(self.account)
        )

        try:
            bucket = s3_resource.Bucket(self.bucket_name)

            # Delete in batches of 5000 objects, to not block the runner loop
            delete_response = bucket.object_versions.limit(count=5000).delete()

            if len(delete_response) == 0:
                # Nothing was deleted, so bucket is empty
                self.set_complete(
                    "No objects remain in bucket '{}'".format(self.bucket_name)
                )
            else:
                num_deleted = sum(len(item["Deleted"]) for item in delete_response)
                log.debug(
                    "Deleted {} objects from bucket '{}'", num_deleted, self.bucket_name
                )

        except ClientError as e:
            if "does not exist" in e.response["Error"]["Message"]:
                # Bucket doesn't exist - treat as successfully emptied bucket
                self.set_complete(
                    "Bucket '{}' does not exist, treating as success".format(
                        self.bucket_name
                    )
                )
            else:
                raise
