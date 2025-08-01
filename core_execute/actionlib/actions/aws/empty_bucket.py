"""Empty an S3 bucket action for Core Execute automation platform."""

from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator

from botocore.exceptions import ClientError

import core_logging as log

from core_framework.models import DeploymentDetails, ActionSpec, ActionParams

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


class EmptyBucketActionParams(ActionParams):
    """
    Parameters for the EmptyBucketAction.

    Attributes
    ----------
    account : str
        The AWS account ID where the bucket is located.
    region : str
        The AWS region where the bucket is located.
    bucket_name : str
        The name of the S3 bucket to empty.
    """

    bucket_name: str = Field(
        ...,
        alias="BucketName",
        description="The name of the bucket to empty (required)",
    )


class EmptyBucketActionSpec(ActionSpec):
    """
    Action specification for the EmptyBucket action.

    Provides validation and default values for EmptyBucket action definitions.
    """

    @model_validator(mode="before")
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """
        Validate and set default parameters for the EmptyBucketActionSpec.

        :param values: Input values dictionary.
        :type values: dict[str, Any]
        :return: Validated values with defaults applied.
        :rtype: dict[str, Any]
        """
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
    """
    Empty an S3 bucket action implementation.

    This action will empty an S3 bucket by deleting all objects and object versions.
    The action processes deletions in batches of 5000 objects to avoid blocking
    the runner loop for extended periods.

    Attributes
    ----------
    params : EmptyBucketActionParams
        Validated parameters for the action.

    Parameters
    ----------
    Kind : str
        Use the value: ``AWS::EmptyBucket``
    Params.Account : str
        The AWS account where the bucket is located
    Params.Region : str
        The AWS region where the bucket is located
    Params.BucketName : str
        The name of the bucket to empty (required)

    Examples
    --------
    ActionSpec YAML configuration:

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
        """
        Initialize the EmptyBucketAction.

        :param definition: The action specification definition.
        :type definition: ActionSpec
        :param context: Execution context for variable resolution.
        :type context: dict[str, Any]
        :param deployment_details: Details about the current deployment.
        :type deployment_details: DeploymentDetails
        :raises ValidationError: If action parameters are invalid.
        """
        super().__init__(definition, context, deployment_details)

        # Validate the action parameters
        self.params = EmptyBucketActionParams(**definition.params)

    def _execute(self):
        """
        Execute the bucket emptying operation.

        Initiates the process of deleting all objects in the specified bucket.
        Sets the action status to running and calls the internal empty bucket method.
        """
        log.trace("EmptyBucketAction._execute()")

        if self.params.bucket_name:  # Fixed: Use snake_case attribute
            self.set_running("Deleting all objects in bucket '{}'".format(self.params.bucket_name))
            self.__empty_bucket()
        else:
            self.set_complete("No bucket specified")

        log.trace("EmptyBucketAction._execute() complete")

    def _check(self):
        """
        Check the status of the bucket emptying operation.

        Continues the bucket emptying process, typically called in subsequent
        iterations to process remaining objects in batches.
        """
        log.trace("EmptyBucketAction._check()")

        self.__empty_bucket()

        log.trace("EmptyBucketAction._check() complete")

    def _unexecute(self):
        """
        Reverse the bucket emptying operation.

        Note: This operation cannot be reversed as deleted objects cannot be restored.
        This method is provided for interface compliance but performs no action.
        """
        pass

    def _cancel(self):
        """
        Cancel the bucket emptying operation.

        Note: Object deletions that have already occurred cannot be undone.
        This method is provided for interface compliance but performs no action.
        """
        pass

    def _resolve(self):
        """
        Resolve template variables in action parameters.

        Uses the renderer to substitute variables in the account, region,
        and bucket_name parameters using the current execution context.
        """
        log.trace("EmptyBucketAction._resolve()")

        self.params.region = self.renderer.render_string(self.params.region, self.context)
        self.params.account = self.renderer.render_string(self.params.account, self.context)
        self.params.bucket_name = self.renderer.render_string(self.params.bucket_name, self.context)

        log.trace("EmptyBucketAction._resolve() complete")

    def __empty_bucket(self):
        """
        Internal method to perform the actual bucket emptying operation.

        Connects to S3 using the provisioning role and deletes objects in batches
        of 5000 to avoid blocking the runner loop. Handles both regular objects
        and versioned objects. Saves progress to state and outputs operation details.

        :raises ClientError: If S3 operations fail (except for non-existent buckets).
        """
        log.trace("EmptyBucketAction.__empty_bucket()")

        # Initialize state tracking if not already present
        if self.get_state("bucket_name"):
            self.set_state("bucket_name", self.params.bucket_name)
            self.set_state("total_objects_deleted", 0)
            self.set_state("batch_count", 0)
            self.set_state("start_time", util.get_current_timestamp())

        # Obtain an S3 resource with assumed role
        s3_resource = aws.s3_resource(
            region=self.params.region,
            role=util.get_provisioning_role_arn(self.params.account),
        )

        try:
            bucket = s3_resource.Bucket(self.params.bucket_name)

            # Delete in batches of 5000 objects, to not block the runner loop
            delete_response = bucket.object_versions.limit(count=5000).delete()

            if len(delete_response) == 0:
                # Nothing was deleted, so bucket is empty
                completion_time = util.get_current_timestamp()
                self.set_state("completion_time", completion_time)
                self.set_state("status", "completed")

                # Set outputs for successful completion
                self.set_output("bucket_name", self.params.bucket_name)
                self.set_output("region", self.params.region)
                self.set_output("account", self.params.account)
                self.set_output("total_objects_deleted", self.get_state("total_objects_deleted"))
                self.set_output("total_batches", self.get_state("batch_count"))
                self.set_output("start_time", self.get_state("start_time"))
                self.set_output("completion_time", completion_time)
                self.set_output("status", "success")
                self.set_output("message", f"Bucket '{self.params.bucket_name}' is now empty")

                self.set_complete("No objects remain in bucket '{}'".format(self.params.bucket_name))
            else:
                # Objects were deleted, update state and continue
                batch_deleted = sum(len(item["Deleted"]) for item in delete_response)
                self.set_state(
                    "total_objects_deleted",
                    self.get_state("total_objects_deleted", 0) + batch_deleted,
                )
                self.set_state("batch_count", self.get_state("batch_count", 0) + 1)

                log.debug(
                    "Deleted {} objects from bucket '{}' (batch {}, total: {})",
                    batch_deleted,
                    self.params.bucket_name,
                    self.get_state("batch_count"),
                    self.get_state("total_objects_deleted"),
                )

                # Update running status with progress
                self.set_running(
                    "Deleted {} objects from bucket '{}' (batch {}, total: {})".format(
                        batch_deleted,
                        self.params.bucket_name,
                        self.get_state("batch_count", 0),
                        self.get_state("total_objects_deleted", 0),
                    )
                )

                # Set intermediate outputs
                self.set_output("bucket_name", self.params.bucket_name)
                self.set_output("region", self.params.region)
                self.set_output("account", self.params.account)
                self.set_output("total_objects_deleted", self.get_state("total_objects_deleted"))
                self.set_output("current_batch", self.get_state("batch_count"))
                self.set_output("last_batch_deleted", batch_deleted)
                self.set_output("start_time", self.get_state("start_time"))
                self.set_output("status", "in_progress")
                self.set_output(
                    "message",
                    f"Deleting objects from bucket '{self.params.bucket_name}' in batches",
                )

        except ClientError as e:
            if "does not exist" in e.response["Error"]["Message"]:
                # Bucket doesn't exist - treat as successfully emptied bucket
                completion_time = util.get_current_timestamp()
                self.set_state("completion_time", completion_time)
                self.set_state("status", "completed_not_found")

                log.warning("Bucket '{}' does not exist", self.params.bucket_name)

                # Set outputs for non-existent bucket
                self.set_output("bucket_name", self.params.bucket_name)
                self.set_output("region", self.params.region)
                self.set_output("account", self.params.account)
                self.set_output("total_objects_deleted", 0)
                self.set_output("total_batches", 0)
                self.set_output("start_time", self.get_state("start_time"))
                self.set_output("completion_time", completion_time)
                self.set_output("status", "success")
                self.set_output(
                    "message",
                    f"Bucket '{self.params.bucket_name}' does not exist, treating as success",
                )

                self.set_complete("Bucket '{}' does not exist, treating as success".format(self.params.bucket_name))
            else:
                # Set error state and outputs
                error_time = util.get_current_timestamp()
                self.set_state("error_time", error_time)
                self.set_state("status", "error")
                self.set_state("error_message", str(e))

                self.set_output("bucket_name", self.params.bucket_name)
                self.set_output("region", self.params.region)
                self.set_output("account", self.params.account)
                self.set_output("total_objects_deleted", self.get_state("total_objects_deleted", 0))
                self.set_output("total_batches", self.get_state("batch_count", 0))
                self.set_output("start_time", self.get_state("start_time"))
                self.set_output("error_time", error_time)
                self.set_output("status", "error")
                self.set_output("error_message", str(e))
                self.set_output("message", f"Error emptying bucket '{self.params.bucket_name}': {e}")

                log.error("Error emptying bucket '{}': {}", self.params.bucket_name, e)
                raise

        log.trace("EmptyBucketAction.__empty_bucket() complete")

    @classmethod
    def generate_action_spec(cls, **kwargs) -> EmptyBucketActionSpec:
        return EmptyBucketActionSpec(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> EmptyBucketActionParams:
        return EmptyBucketActionParams(**kwargs)
