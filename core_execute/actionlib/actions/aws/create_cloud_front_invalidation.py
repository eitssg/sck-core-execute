"""Create CloudFront invalidation action to clear the cache"""

from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator
from core_framework.models import ActionSpec, DeploymentDetails

import core_helper.aws as aws

import core_logging as log

import core_framework as util
from core_execute.actionlib.action import BaseAction

from datetime import datetime as dt


class CreateCloudFrontInvalidationActionParams(BaseModel):
    """Parameters for the CreateCloudFrontInvalidationAction"""

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
    """Generate the action definition"""

    @model_validator(mode="before")
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Validate the parameters for the CreateCloudFrontInvalidationActionSpec"""
        if not (values.get("name") or values.get("Name")):
            values["name"] = "action-aws-createcloudfrontinvalidation-name"
        if not (values.get("kind") or values.get("kind")):
            values["kind"] = "AWS::CreateCloudFrontInvalidation"
        if not (values.get("depends_on") or values.get("DependsOn")):
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
    """Create a CloudFront invalidation and clear caches

    This action will create a CloudFront invalidation and clear the cache.  The action will wait for the invalidation to complete before returning.

    Attributes:
        kind: Use the value: ``AWS::CreateCloudFrontInvalidation``
        Params.Account: The account where CloudFront is located
        Params.Region: The region where CloudFront is located
        Params.DistributionId: The ID of the CloudFront distribution to invalidate (required)
        Params.Paths: The paths to invalidate (optional).  Defaults to ['*']

    .. rubric:: ActionSpec:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Name: action-aws-createcloudfrontinvalidation-name
              Kind: "AWS::CreateCloudFrontInvalidation"
              Params:
                  Account: "123456789012"
                  Region: "ap-southeast-1"
                  DistributionId: "E1234567890"
                  Paths:
                    - "/index.html"
                    - "/images/*"
              Scope: "build"

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

    def _execute(self):

        log.trace("Executing CreateCloudFrontInvalidationAction")

        if self.params.DistributionId is None or self.params.distribution_id == "":
            self.set_complete("No distribution specified")
            log.warning("No distribution specified")
            return

        # Obtain a CloudFront client
        cloudfront_client = aws.cloudfront_client(
            region=self.params.Region,
            role=util.get_provisioning_role_arn(self.params.account),
        )

        response = cloudfront_client.create_invalidation(
            DistributionId=self.params.distribution_id,
            InvalidationBatch={
                "Paths": {
                    "Items": self.params.paths,
                    "Quantity": len(self.params.paths),
                },
                "CallerReference": dt.utcnow().isoformat(),
            },
        )

        self.set_state("InvalidationId", response["Invalidation"]["Id"])
        self.set_complete(
            "Invalidation has been triggered - '{}'".format(
                response["Invalidation"]["Id"]
            )
        )

        log.trace(
            "Invalidation has been triggered - '{}'", response["Invalidation"]["Id"]
        )

    def _check(self):
        self.set_failed("Internal error - _check() should not have been called")

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):

        log.trace("Resolving CreateCloudFrontInvalidationAction")

        self.params.region = self.renderer.render_string(
            self.params.region, self.context
        )
        self.params.account = self.renderer.render_string(
            self.params.account, self.context
        )
        self.params.distribution_id = self.renderer.render_string(
            self.params.distribution_id, self.context
        )
        paths = []
        for path in self.params.paths:
            paths.append(self.renderer.render_string(path, self.context))
        self.params.paths = paths

        log.trace("CreateCloudFrontInvalidationAction resolved")
