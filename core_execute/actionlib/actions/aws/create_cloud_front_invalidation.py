"""Create CloudFront invalidation action to clear the cache"""

from typing import Any
from pydantic import Field, model_validator
from core_framework.models import ActionSpec, ActionParams, DeploymentDetails

import core_helper.aws as aws

import core_logging as log

import core_framework as util
from core_execute.actionlib.action import BaseAction


class CreateCloudFrontInvalidationActionParams(ActionParams):
    """
    Parameters for the CreateCloudFrontInvalidationAction.

    :param account: The account to use for the action (required)
    :type account: str
    :param region: The region where CloudFront is located (required)
    :type region: str
    :param distribution_id: The CloudFront distribution ID to invalidate (required)
    :type distribution_id: str
    :param paths: The paths to invalidate (optional). Defaults to ['*']
    :type paths: list[str]
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


class CreateCloudFrontInvalidationActionSpec(ActionSpec):
    """
    Generate the action definition for CreateCloudFrontInvalidationAction.

    This class provides default values and validation for CreateCloudFrontInvalidationAction parameters.

    :param values: Dictionary of action specification values
    :type values: dict[str, Any]
    :return: Validated action specification values
    :rtype: dict[str, Any]
    """

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """
        Validate the parameters for the CreateCloudFrontInvalidationActionSpec.

        :param values: Input values for validation
        :type values: dict[str, Any]
        :return: Validated and potentially modified values
        :rtype: dict[str, Any]
        """
        if not (values.get("name") or values.get("Name")):
            values["name"] = "action-aws-createcloudfrontinvalidation-name"
        if not (values.get("kind") or values.get("Kind")):
            values["kind"] = "AWS::CreateCloudFrontInvalidation"
        if not values.get("depends_on", values.get("DependsOn")):  # arrays are falsy if empty
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Params")):
            values["params"] = {
                "account": "",
                "region": "",
                "distribution_id": "",
                "paths": ["*"],
            }
        return values


class CreateCloudFrontInvalidationAction(BaseAction):
    """
    Create a CloudFront invalidation to clear cache.

    This action creates a CloudFront invalidation request to clear cached content.
    The action completes immediately after triggering the invalidation without waiting
    for completion.

    :param definition: The action specification containing configuration details
    :type definition: ActionSpec
    :param context: The Jinja2 rendering context containing all variables
    :type context: dict[str, Any]
    :param deployment_details: Client/portfolio/app/branch/build information
    :type deployment_details: DeploymentDetails

    .. rubric:: Parameters

    :Name: Enter a name to define this action instance
    :Kind: Use the value ``AWS::CreateCloudFrontInvalidation``
    :Params.Account: The account where CloudFront is located (required)
    :Params.Region: The region where CloudFront is located (required)
    :Params.DistributionId: The ID of the CloudFront distribution to invalidate (required)
    :Params.Paths: List of paths to invalidate (optional, defaults to ['*'])

    .. rubric:: ActionSpec Example

    .. code-block:: yaml

        - Name: action-aws-createcloudfrontinvalidation-name
          Kind: "AWS::CreateCloudFrontInvalidation"
          Params:
            Account: "123456789012"
            Region: "us-east-1"
            DistributionId: "E1234567890"
            Paths:
              - "/index.html"
              - "/images/*"
              - "/css/*"
          Scope: "build"

    .. note::
        CloudFront invalidations are asynchronous. This action only triggers the
        invalidation and does not wait for completion.

    .. warning::
        CloudFront charges for invalidations beyond the monthly free tier.
        Using "/*" will invalidate all content.
    """

    def __init__(
        self,
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        # Validate the action definition parameters
        self.params = CreateCloudFrontInvalidationActionParams(**definition.params)

    def _resolve(self):
        """
        Resolve template variables in action parameters.

        This method renders Jinja2 templates in the action parameters using the current context.
        """
        log.trace("Resolving CreateCloudFrontInvalidationAction")

        self.params.region = self.renderer.render_string(self.params.region, self.context)
        self.params.account = self.renderer.render_string(self.params.account, self.context)
        self.params.distribution_id = self.renderer.render_string(self.params.distribution_id, self.context)

        # Render each path in the paths list
        rendered_paths = []
        for path in self.params.paths:
            rendered_paths.append(self.renderer.render_string(path, self.context))
        self.params.paths = rendered_paths

        log.trace("CreateCloudFrontInvalidationAction resolved")

    def _execute(self):
        """
        Execute the CloudFront invalidation operation.

        This method creates a CloudFront invalidation request for the specified
        distribution and paths. It sets appropriate state outputs for tracking.

        :raises: Sets action to failed if distribution ID is missing or CloudFront operation fails
        """
        log.trace("Executing CreateCloudFrontInvalidationAction")

        # Validate required parameters
        if not self.params.distribution_id or self.params.distribution_id == "":
            self.set_failed("DistributionId parameter is required")
            log.error("DistributionId parameter is required")
            return

        # Set initial state information
        self.set_state("DistributionId", self.params.distribution_id)
        self.set_state("InvalidationPaths", self.params.paths)
        self.set_state("Region", self.params.region)
        self.set_state("Account", self.params.account)

        # Set outputs for other actions to reference
        self.set_output("DistributionId", self.params.distribution_id)
        self.set_output("InvalidationPaths", self.params.paths)

        # Obtain a CloudFront client
        try:
            cloudfront_client = aws.cloudfront_client(
                region=self.params.region,
                role=util.get_provisioning_role_arn(self.params.account),
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
                DistributionId=self.params.distribution_id,
                InvalidationBatch={
                    "Paths": {
                        "Items": self.params.paths,
                        "Quantity": len(self.params.paths),
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
        """
        Check the status of the CloudFront invalidation.

        .. note::
            This action completes immediately after creating the invalidation.
            Status checking is not typically needed but can be implemented if required.
        """
        # Get the invalidation ID from state
        invalidation_id = self.get_state("InvalidationId")
        if not invalidation_id:
            self.set_failed("No invalidation ID found in state")
            return

        try:
            cloudfront_client = aws.cloudfront_client(
                region=self.params.region,
                role=util.get_provisioning_role_arn(self.params.account),
            )
        except Exception as e:
            log.error("Failed to create CloudFront client for status check: {}", e)
            self.set_failed(f"Failed to create CloudFront client: {e}")
            return

        try:
            response = cloudfront_client.get_invalidation(DistributionId=self.params.distribution_id, Id=invalidation_id)
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
        """
        Rollback the CloudFront invalidation operation.

        .. note::
            CloudFront invalidations cannot be cancelled or undone once created.
            This method is a no-op.
        """
        log.trace("CreateCloudFrontInvalidationAction unexecute - no action required")
        self.set_complete("Invalidation cannot be undone")

    def _cancel(self):
        """
        Cancel the CloudFront invalidation operation.

        .. note::
            CloudFront invalidations cannot be cancelled once created.
            This method is a no-op.
        """
        log.trace("CreateCloudFrontInvalidationAction cancel - no action required")
        self.set_complete("Invalidation cannot be cancelled")

    @classmethod
    def generate_action_spec(cls, **kwargs) -> CreateCloudFrontInvalidationActionSpec:
        return CreateCloudFrontInvalidationActionSpec(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> CreateCloudFrontInvalidationActionParams:
        return CreateCloudFrontInvalidationActionParams(**kwargs)
