"""Create CloudFront invalidation action to clear the cache"""

from typing import Any
from core_framework.models import ActionDefinition, DeploymentDetails, ActionParams

import core_helper.aws as aws

import core_logging as log

import core_framework as util
from core_execute.actionlib.action import BaseAction

from datetime import datetime as dt


def generate_template() -> ActionDefinition:
    """Generate the action definition"""

    definition = ActionDefinition(
        Label="action-definition-label",
        Type="AWS::CreateCloudFrontInvalidation",
        DependsOn=["put-a-label-here"],
        Params=ActionParams(
            Account="The account to use for the action (required)",
            Region="The region to copy the image to (required)",
            DistributionId="The CloudFront distribution ID (required)",
            Paths=["The paths to invalidate (optional). Defaults to ['*']"],
        ),
        Scope="Based on your deployment details, it one of 'portfolio', 'app', 'branch', or 'build'",
    )

    return definition


class CreateCloudFrontInvalidationAction(BaseAction):
    """Create a CloudFront invalidation and clear caches

    This action will create a CloudFront invalidation and clear the cache.  The action will wait for the invalidation to complete before returning.

    Attributes:
        Type: Use the value: ``AWS::CreateCloudFrontInvalidation``
        Params.Account: The account where CloudFront is located
        Params.Region: The region where CloudFront is located
        Params.DistributionId: The ID of the CloudFront distribution to invalidate (required)
        Params.Paths: The paths to invalidate (optional).  Defaults to ['*']

    .. rubric:: ActionDefinition:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Label: action-aws-createcloudfrontinvalidation-label
              Type: "AWS::CreateCloudFrontInvalidation"
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
        definition: ActionDefinition,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        if self.params.Paths is None:
            self.params.Paths = ["*"]

    def _execute(self):

        log.trace("Executing CreateCloudFrontInvalidationAction")

        if self.params.DistributionId is None or self.params.DistributionId == "":
            self.set_complete("No distribution specified")
            log.warning("No distribution specified")
            return

        # Obtain a CloudFront client
        cloudfront_client = aws.cloudfront_client(
            region=self.params.Region,
            role=util.get_provisioning_role_arn(self.params.Account),
        )

        response = cloudfront_client.create_invalidation(
            DistributionId=self.params.DistributionId,
            InvalidationBatch={
                "Paths": {
                    "Items": self.params.Paths,
                    "Quantity": len(self.params.Paths),
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

        self.params.Region = self.renderer.render_string(
            self.params.Region, self.context
        )
        self.params.Account = self.renderer.render_string(
            self.params.Account, self.context
        )
        self.params.DistributionId = self.renderer.render_string(
            self.params.DistributionId, self.context
        )
        paths = []
        for path in self.params.Paths:
            paths.append(self.renderer.render_string(path, self.context))
        self.params.Paths = paths

        log.trace("CreateCloudFrontInvalidationAction resolved")
