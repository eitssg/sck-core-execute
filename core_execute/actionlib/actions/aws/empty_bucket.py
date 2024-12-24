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
        DependsOn=["put-a-label-here"],
        Params=ActionParams(
            Account="The account to use for the action (required)",
            Region="The region to create the stack in (required)",
            BucketName="The name of the bucket to empty (required)",
        ),
        Scope="Based on your deployment details, it one of 'portfolio', 'app', 'branch', or 'build'",
    )

    return definition


class EmptyBucketAction(BaseAction):
    """Empty an S3 bucket

    This action will empty an S3 bucket.  The action will wait for the deletion to complete before returning.

    Attributes:
        Type: Use the value: ``AWS::EmptyBucket``
        Params.Account: The account where the bucket is located
        Params.Region: The region where the bucket is located
        Params.BucketName: The name of the bucket to empty (required)

    .. rubric: ActionDefinition:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Label: action-aws-emptybucket-label
              Type: "AWS::EmptyBucket"
              Params:
                Account: "154798051514"
                Region: "ap-southeast-1"
                BucketName: "my-bucket-name"
              Scope: "build"

    """

    def __init__(
        self,
        definition: ActionDefinition,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

    def _execute(self):

        log.trace("EmptyBucketAction._execute()")

        if self.params.BucketName:
            self.set_running(
                "Deleting all objects in bucket '{}'".format(self.params.BucketName)
            )
            self.__empty_bucket()
        else:
            self.set_complete("No bucket specified")

        log.trace("EmptyBucketAction._execute()")

    def _check(self):

        log.trace("EmptyBucketAction._check()")

        self.__empty_bucket()

        log.trace("EmptyBucketAction._check()")

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):

        log.trace("EmptyBucketAction._resolve()")

        self.params.Region = self.renderer.render_string(
            self.params.Region, self.context
        )
        self.params.Account = self.renderer.render_string(
            self.params.Account, self.context
        )
        self.params.BucketName = self.renderer.render_string(
            self.params.BucketName, self.context
        )

        log.trace("EmptyBucketAction._resolve()")

    def __empty_bucket(self):

        log.trace("EmptyBucketAction.__empty_bucket()")

        # Obtain a CloudFormation client
        s3_resource = aws.s3_resource(
            region=self.params.Region,
            role=util.get_provisioning_role_arn(self.params.Account),
        )

        try:
            bucket = s3_resource.Bucket(self.params.BucketName)

            # Delete in batches of 5000 objects, to not block the runner loop
            delete_response = bucket.object_versions.limit(count=5000).delete()

            if len(delete_response) == 0:
                # Nothing was deleted, so bucket is empty
                self.set_complete(
                    "No objects remain in bucket '{}'".format(self.params.BucketName)
                )
            else:
                num_deleted = sum(len(item["Deleted"]) for item in delete_response)
                log.debug(
                    "Deleted {} objects from bucket '{}'",
                    num_deleted,
                    self.params.BucketName,
                )

        except ClientError as e:
            if "does not exist" in e.response["Error"]["Message"]:
                # Bucket doesn't exist - treat as successfully emptied bucket
                log.warning("Bucket '{}' does not exist", self.params.BucketName)
                self.set_complete(
                    "Bucket '{}' does not exist, treating as success".format(
                        self.params.BucketName
                    )
                )
            else:
                log.error("Error emptying bucket '{}': {}", self.params.BucketName, e)
                raise

        log.trace("EmptyBucketAction.__empty_bucket() complete")
