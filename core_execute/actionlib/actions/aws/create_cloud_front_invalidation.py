from typing import Any
from core_framework.models import ActionDefinition, DeploymentDetails

import core_helper.aws as aws

import core_execute.envinfo as envinfo
from core_execute.actionlib.action import BaseAction

from datetime import datetime as dt


class CreateCloudFrontInvalidationAction(BaseAction):
    """Create a CloudFront invalidation and clear caches"""

    def __init__(self, definition: ActionDefinition, context: dict[str, Any], deployment_details: DeploymentDetails):
        super().__init__(definition, context, deployment_details)
        self.account = self.params.Account
        self.region = self.params.Region
        self.distribution_id = self.params.DistributionId
        self.paths = self.params.Paths

    def _execute(self):
        if self.distribution_id is None or self.distribution_id == "":
            self.set_complete("No distribution specified")
            return

        # Obtain a CloudFront client
        cloudfront_client = aws.cloudfront_client(
            region=self.region, role=envinfo.provisioning_role_arn(self.account)
        )

        response = cloudfront_client.create_invalidation(
            DistributionId=self.distribution_id,
            InvalidationBatch={
                "Paths": {"Items": self.paths, "Quantity": len(self.paths)},
                "CallerReference": dt.utcnow().isoformat(),
            },
        )

        self.set_state("InvalidationId", response["Invalidation"]["Id"])
        self.set_complete(
            "Invalidation has been triggered - '{}'".format(
                response["Invalidation"]["Id"]
            )
        )

    def _check(self):
        self.set_failed("Internal error - _check() should not have been called")

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):
        self.region = self.renderer.render_string(self.region, self.context)
        self.account = self.renderer.render_string(self.account, self.context)
        self.distribution_id = self.renderer.render_string(
            self.distribution_id, self.context
        )
        paths = []
        for path in self.paths:
            paths.append(self.renderer.render_string(path, self.context))
        self.paths = paths