"""Create a CloudFront invalidation to clear cached content.

Starts an invalidation for a distribution and exposes details in state/outputs.
"""

from typing import Any
from pydantic import Field, model_validator
from core_framework.models import ActionResource, ActionSpec, DeploymentDetails

import core_helper.aws as aws

import core_logging as log

import core_framework as util
from core_execute.actionlib.action import BaseAction


class CreateCloudFrontInvalidationActionSpec(ActionSpec):
    """Parameters for creating a CloudFront invalidation.

    Args:
      account: AWS account ID used for the action.
      region: AWS region used for API calls (CloudFront is global).
      distribution_id: CloudFront distribution ID to invalidate.
      paths: Paths to invalidate. Defaults to ['*'].
    """

    distribution_id: str = Field(
        ...,
        alias="DistributionId",
        description="The CloudFront distribution ID to invalidate (required)",
    )
    paths: list[str] = Field(
        default_factory=lambda: ["*"],
        alias="Paths",
        description="The paths to invalidate (optional). Defaults to ['*']",
    )


class CreateCloudFrontInvalidationActionResource(ActionResource):
    """Resource model for CreateCloudFrontInvalidation (normalizes kind/spec)."""

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Normalize incoming values and enforce canonical kind/spec."""
        if not isinstance(values, dict):
            return values

        values.pop("kind", None)
        values.pop("Kind", None)
        values["kind"] = "AWS::CreateCloudFrontInvalidation"

        spec = values.pop("spec", None) or values.pop("Spec", None)
        if isinstance(spec, dict):
            values["spec"] = spec
        elif isinstance(spec, CreateCloudFrontInvalidationActionSpec):
            values["spec"] = spec.model_dump()

        return values


class CreateCloudFrontInvalidationAction(BaseAction[CreateCloudFrontInvalidationActionSpec]):
    """Create a CloudFront invalidation to clear cached content.

    Completes after requesting the invalidation; does not wait for completion.

    Args:
      definition: The action specification containing configuration.
      context: Jinja2 rendering context for template variables.
      deployment_details: Deployment metadata for this run.

    """

    def __init__(
        self,
        definition: ActionResource,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        # Validate the action definition parameters
        self.spec = CreateCloudFrontInvalidationActionSpec(**definition.spec)

    def _resolve(self):
        """Render templates for region, account, distribution_id, and paths."""
        log.trace("Resolving CreateCloudFrontInvalidationAction")

        self.spec.region = self.renderer.render_string(self.spec.region, self.context)
        self.spec.account = self.renderer.render_string(self.spec.account, self.context)
        self.spec.distribution_id = self.renderer.render_string(self.spec.distribution_id, self.context)

        # Render each path in the paths list
        rendered_paths = []
        for path in self.spec.paths:
            rendered_paths.append(self.renderer.render_string(path, self.context))
        self.spec.paths = rendered_paths

        log.trace("CreateCloudFrontInvalidationAction resolved")

    def _execute(self):
        """Create the invalidation and set state/outputs.

        Raises:
          Sets failed status if DistributionId is missing or CloudFront calls fail.
        """
        log.trace("Executing CreateCloudFrontInvalidationAction")

        # Validate required parameters
        if not self.spec.distribution_id or self.spec.distribution_id == "":
            self.set_failed("DistributionId parameter is required")
            log.error("DistributionId parameter is required")
            return

        # Set initial state information
        self.set_state("DistributionId", self.spec.distribution_id)
        self.set_state("InvalidationPaths", self.spec.paths)
        self.set_state("Region", self.spec.region)
        self.set_state("Account", self.spec.account)

        # Set outputs for other actions to reference
        self.set_output("DistributionId", self.spec.distribution_id)
        self.set_output("InvalidationPaths", self.spec.paths)

        # Obtain a CloudFront client
        try:
            cloudfront_client = aws.cloudfront_client(
                region=self.spec.region,
                role=util.get_provisioning_role_arn(self.spec.account),
            )
        except Exception as e:
            log.error("Failed to create CloudFront client: {}", e)
            self.set_failed(f"Failed to create CloudFront client: {e}")
            return

        # Create the invalidation request
        caller_reference = f"invalidate at {util.get_current_timestamp()}"

        self.set_running("Creating CloudFront invalidation")

        try:
            response = cloudfront_client.create_invalidation(
                DistributionId=self.spec.distribution_id,
                InvalidationBatch={
                    "Paths": {
                        "Items": self.spec.paths,
                        "Quantity": len(self.spec.paths),
                    },
                    "CallerReference": caller_reference,
                },
            )
        except Exception as e:
            log.error("Failed to create CloudFront invalidation: {}", e)
            self.set_failed(f"Failed to create CloudFront invalidation: {e}")
            return

        # Extract invalidation details from response
        invalidation = response["Invalidation"]
        invalidation_id = invalidation["Id"]
        invalidation_status = invalidation["Status"]
        creation_time = invalidation["CreateTime"].isoformat() if invalidation.get("CreateTime") else None

        # Set comprehensive state outputs
        self.set_state("InvalidationId", invalidation_id)
        self.set_state("InvalidationStatus", invalidation_status)
        self.set_state("CallerReference", caller_reference)
        self.set_state("CreationTime", creation_time)
        self.set_state("InvalidationStarted", True)

        # Set outputs for other actions to reference
        self.set_output("InvalidationId", invalidation_id)
        self.set_output("InvalidationStatus", invalidation_status)
        self.set_output("InvalidationStarted", True)

        log.debug("CloudFront invalidation created successfully: {}", invalidation_id)
        self.set_complete("Invalidation created successfully")

        log.trace("CreateCloudFrontInvalidationAction completed")

    def _check(self):
        """Optionally check the invalidation status and update state/outputs."""
        # Get the invalidation ID from state
        invalidation_id = self.get_state("InvalidationId")
        if not invalidation_id:
            self.set_failed("No invalidation ID found in state")
            return

        try:
            cloudfront_client = aws.cloudfront_client(
                region=self.spec.region,
                role=util.get_provisioning_role_arn(self.spec.account),
            )
        except Exception as e:
            log.error("Failed to create CloudFront client for status check: {}", e)
            self.set_failed(f"Failed to create CloudFront client: {e}")
            return

        try:
            response = cloudfront_client.get_invalidation(DistributionId=self.spec.distribution_id, Id=invalidation_id)
        except Exception as e:
            log.error("Failed to get invalidation status: {}", e)
            self.set_failed(f"Failed to get invalidation status: {e}")
            return

        invalidation = response["Invalidation"]
        status = invalidation["Status"]

        # Update state with current status
        self.set_state("InvalidationStatus", status)
        self.set_output("InvalidationStatus", status)

        if status == "Completed":
            self.set_state("InvalidationCompleted", True)
            self.set_output("InvalidationCompleted", True)
            self.set_complete("Invalidation completed successfully")
        elif status == "InProgress":
            self.set_running("Invalidation is in progress")
        else:
            log.warning("Unknown invalidation status: {}", status)
            self.set_running(f"Invalidation status: {status}")

    def _unexecute(self):
        """No rollback; CloudFront invalidations cannot be undone."""
        log.trace("CreateCloudFrontInvalidationAction unexecute - no action required")
        self.set_complete("Invalidation cannot be undone")

    def _cancel(self):
        """No-op; CloudFront invalidations cannot be cancelled once created."""
        log.trace("CreateCloudFrontInvalidationAction cancel - no action required")
        self.set_complete("Invalidation cannot be cancelled")

    @classmethod
    def generate_action_resource(cls, **kwargs) -> CreateCloudFrontInvalidationActionResource:
        """Factory: create a typed CreateCloudFrontInvalidationActionResource."""
        return CreateCloudFrontInvalidationActionResource(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> CreateCloudFrontInvalidationActionSpec:
        """Factory: create typed CreateCloudFrontInvalidationActionSpec."""
        return CreateCloudFrontInvalidationActionSpec(**kwargs)
