"""Empty and S3 bucket"""

from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator

from botocore.exceptions import ClientError

import core_logging as log

from core_framework.models import DeploymentDetails, ActionSpec

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


class EmptyBucketActionParams(BaseModel):
    """Parameters for the EmptyBucketAction"""

    model_config = ConfigDict(populate_by_name=True, validate_assignment=True)

    account: str = Field(
        ..., alias="Account", description="The account to use for the action (required)"
    )
    region: str = Field(
        ..., alias="Region", description="The region to create the stack in (required)"
    )
    bucket_name: str = Field(
        ...,
        alias="BucketName",
        description="The name of the bucket to empty (required)",
    )


class EmptyBucketActionSpec(ActionSpec):
    """Generate the action definition"""

    @model_validator(mode="before")
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Validate the parameters for the EmptyBucketActionSpec"""
        if not (values.get("name") or values.get("Name")):
            values["name"] = "action-aws-emptybucket-name"
        if not (values.get("kind") or values.get("Kind")):
            values["kind"] = "AWS::EmptyBucket"
        if not (values.get("depends_on") or values.get("DependsOn")):
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Params")):
            values["params"] = {
                "account": "",
                "region": "",
                "bucket_name": "",
            }
        return values


class EmptyBucketAction(BaseAction):
    """Empty an S3 bucket

    This action will empty an S3 bucket.  The action will wait for the deletion to complete before returning.

    Attributes:
        Kind: Use the value: ``AWS::EmptyBucket``
        Params.Account: The account where the bucket is located
        Params.Region: The region where the bucket is located
        Params.BucketName: The name of the bucket to empty (required)

    .. rubric: ActionSpec:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Name: action-aws-emptybucket-name
              Kind: "AWS::EmptyBucket"
              Params:
                Account: "154798051514"
                Region: "ap-southeast-1"
                BucketName: "my-bucket-name"
              Scope: "build"

    """

    def __init__(
        self,
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        # Validate the action parameters
        self.params = EmptyBucketActionParams(**definition.params)

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
