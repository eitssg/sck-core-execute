"""Empty an S3 bucket by deleting all objects and versions (batched)."""

from typing import Any
from pydantic import Field, model_validator

from botocore.exceptions import ClientError

import core_logging as log

from core_framework.models import DeploymentDetails, ActionResource, ActionSpec

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


class EmptyBucketActionSpec(ActionSpec):
    """Parameters for the EmptyBucket action.

    Attributes:
      account: AWS account ID where the bucket resides.
      region: AWS region of the bucket.
      bucket_name: Name of the S3 bucket to empty.
    """

    bucket_name: str = Field(
        ...,
        alias="BucketName",
        description="The name of the bucket to empty (required)",
    )


class EmptyBucketActionResource(ActionResource):
    """Resource model for EmptyBucket (normalizes kind/spec)."""

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Normalize incoming values and set kind to 'AWS::EmptyBucket'."""
        if not isinstance(values, dict):
            return values

        values.pop("kind", None)
        values.pop("Kind", None)
        values["kind"] = "AWS::EmptyBucket"

        spec = values.pop("spec", None) or values.pop("Spec", None)
        if isinstance(spec, dict):
            values["spec"] = spec
        elif isinstance(spec, EmptyBucketActionSpec):
            values["spec"] = spec.model_dump()

        return values


class EmptyBucketAction(BaseAction[EmptyBucketActionSpec]):
    """Delete all objects and versions from an S3 bucket in batches.

    Runs in chunks (up to 5000 versions per iteration) to avoid blocking the runner.
    Progress is tracked in state and updated across _execute/_check calls.
    """

    def __init__(
        self,
        definition: ActionResource,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        """Initialize the action and validate parameters.

        Args:
          definition: Action resource containing metadata and spec.
          context: Rendering context for template variables.
          deployment_details: Deployment metadata (portfolio/app/branch/build).

        """
        super().__init__(definition, context, deployment_details)

        # Validate the action parameters
        self.spec = EmptyBucketActionSpec(**definition.spec)

    def _execute(self):
        """Start or continue bucket emptying and set the action status."""
        log.trace("EmptyBucketAction._execute()")

        if self.spec.bucket_name:
            self.set_running(f"Deleting all objects in bucket '{self.spec.bucket_name}'")
            self.__empty_bucket()
        else:
            self.set_complete("No bucket specified")

        log.trace("EmptyBucketAction._execute() complete")

    def _check(self):
        """Continue deleting objects in the next batch (idempotent)."""
        log.trace("EmptyBucketAction._check()")

        self.__empty_bucket()

        log.trace("EmptyBucketAction._check() complete")

    def _unexecute(self):
        """No rollback; deleted objects cannot be restored."""
        pass

    def _cancel(self):
        """No-op; deletions already performed cannot be cancelled."""
        pass

    def _resolve(self):
        """Render template variables for account, region, and bucket_name."""
        log.trace("EmptyBucketAction._resolve()")

        self.spec.region = self.renderer.render_string(self.spec.region, self.context)
        self.spec.account = self.renderer.render_string(self.spec.account, self.context)
        self.spec.bucket_name = self.renderer.render_string(self.spec.bucket_name, self.context)

        log.trace("EmptyBucketAction._resolve() complete")

    def __empty_bucket(self):
        """Delete objects/versions in batches and update state/outputs.

        - Uses the provisioning role for S3 access
        - Deletes up to 5000 object versions per iteration
        - Marks completion when no items remain

        Raises:
          ClientError: For S3 failures other than a non-existent bucket.
        """
        log.trace("EmptyBucketAction.__empty_bucket()")

        # Initialize state tracking if not already present
        if self.get_state("bucket_name"):
            self.set_state("bucket_name", self.spec.bucket_name)
            self.set_state("total_objects_deleted", 0)
            self.set_state("batch_count", 0)
            self.set_state("start_time", util.get_current_timestamp())

        # Obtain an S3 resource with assumed role
        s3_resource = aws.s3_resource(
            region=self.spec.region,
            role_arn=util.get_provisioning_role_arn(self.spec.account),
        )

        try:
            bucket = s3_resource.Bucket(self.spec.bucket_name)

            # Delete in batches of 5000 objects, to not block the runner loop
            delete_response = bucket.object_versions.limit(count=5000).delete()

            if len(delete_response) == 0:
                # Nothing was deleted, so bucket is empty
                completion_time = util.get_current_timestamp()
                self.set_state("completion_time", completion_time)
                self.set_state("status", "completed")

                # Set outputs for successful completion
                self.set_output("bucket_name", self.spec.bucket_name)
                self.set_output("region", self.spec.region)
                self.set_output("account", self.spec.account)
                self.set_output("total_objects_deleted", self.get_state("total_objects_deleted"))
                self.set_output("total_batches", self.get_state("batch_count"))
                self.set_output("start_time", self.get_state("start_time"))
                self.set_output("completion_time", completion_time)
                self.set_output("status", "success")
                self.set_output("message", f"Bucket '{self.spec.bucket_name}' is now empty")

                self.set_complete(f"No objects remain in bucket '{self.spec.bucket_name}'")
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
                    self.spec.bucket_name,
                    self.get_state("batch_count"),
                    self.get_state("total_objects_deleted"),
                )

                # Update running status with progress
                self.set_running(
                    "Deleted {} objects from bucket '{}' (batch {}, total: {})".format(
                        batch_deleted,
                        self.spec.bucket_name,
                        self.get_state("batch_count", 0),
                        self.get_state("total_objects_deleted", 0),
                    )
                )

                # Set intermediate outputs
                self.set_output("bucket_name", self.spec.bucket_name)
                self.set_output("region", self.spec.region)
                self.set_output("account", self.spec.account)
                self.set_output("total_objects_deleted", self.get_state("total_objects_deleted"))
                self.set_output("current_batch", self.get_state("batch_count"))
                self.set_output("last_batch_deleted", batch_deleted)
                self.set_output("start_time", self.get_state("start_time"))
                self.set_output("status", "in_progress")
                self.set_output(
                    "message",
                    f"Deleting objects from bucket '{self.spec.bucket_name}' in batches",
                )

        except ClientError as e:
            if "does not exist" in e.response["Error"]["Message"]:
                # Bucket doesn't exist - treat as successfully emptied bucket
                completion_time = util.get_current_timestamp()
                self.set_state("completion_time", completion_time)
                self.set_state("status", "completed_not_found")

                log.warning("Bucket '{}' does not exist", self.spec.bucket_name)

                # Set outputs for non-existent bucket
                self.set_output("bucket_name", self.spec.bucket_name)
                self.set_output("region", self.spec.region)
                self.set_output("account", self.spec.account)
                self.set_output("total_objects_deleted", 0)
                self.set_output("total_batches", 0)
                self.set_output("start_time", self.get_state("start_time"))
                self.set_output("completion_time", completion_time)
                self.set_output("status", "success")
                self.set_output(
                    "message",
                    f"Bucket '{self.spec.bucket_name}' does not exist, treating as success",
                )

                self.set_complete(f"Bucket '{self.spec.bucket_name}' does not exist, treating as success")
            else:
                # Set error state and outputs
                error_time = util.get_current_timestamp()
                self.set_state("error_time", error_time)
                self.set_state("status", "error")
                self.set_state("error_message", str(e))

                self.set_output("bucket_name", self.spec.bucket_name)
                self.set_output("region", self.spec.region)
                self.set_output("account", self.spec.account)
                self.set_output("total_objects_deleted", self.get_state("total_objects_deleted", 0))
                self.set_output("total_batches", self.get_state("batch_count", 0))
                self.set_output("start_time", self.get_state("start_time"))
                self.set_output("error_time", error_time)
                self.set_output("status", "error")
                self.set_output("error_message", str(e))
                self.set_output("message", f"Error emptying bucket '{self.spec.bucket_name}': {e}")

                log.error("Error emptying bucket '{}': {}", self.spec.bucket_name, e)
                raise

        log.trace("EmptyBucketAction.__empty_bucket() complete")

    @classmethod
    def generate_action_resource(cls, **kwargs) -> EmptyBucketActionResource:
        """Factory: create a typed EmptyBucketActionResource."""
        return EmptyBucketActionResource(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> EmptyBucketActionSpec:
        """Factory: create typed EmptyBucketActionSpec."""
        return EmptyBucketActionSpec(**kwargs)
