from typing import Any

from botocore.exceptions import ClientError

from core_framework.models import ActionDefinition, DeploymentDetails

import core_helper.aws as aws

import core_execute.envinfo as envinfo
from core_execute.actionlib.action import BaseAction


class DeleteEcrRepositoryAction(BaseAction):
    """Delete an ECR repository"""

    def __init__(
        self,
        definition: ActionDefinition,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        self.account = self.params.Account
        self.region = self.params.Region
        self.repository_name = self.params.RepositoryName

    def _execute(self):
        ecr_client = aws.ecr_client(
            region=self.region, role=envinfo.provisioning_role_arn(self.account)
        )

        try:
            ecr_client.delete_repository(
                registryId=self.account, repositoryName=self.repository_name, force=True
            )
            self.set_complete(
                "ECR repository '{}' has been deleted".format(self.repository_name)
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "RepositoryNotFoundException":
                self.set_complete(
                    "ECR repository '{}' does not exist, may have been previously deleted".format(
                        self.repository_name
                    )
                )
            else:
                raise

    def _check(self):
        self.set_failed("Internal error - _check() should not have been called")

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):
        self.account = self.renderer.render_string(self.account, self.context)
        self.region = self.renderer.render_string(self.region, self.context)
        self.repository_name = self.renderer.render_string(
            self.repository_name, self.context
        )
