"""Delete an ECR repository"""
from typing import Any

from botocore.exceptions import ClientError

from core_framework.models import ActionDefinition, DeploymentDetails, ActionParams

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


def generate_template() -> ActionDefinition:
    """Generate the action definition"""

    definition = ActionDefinition(
        Label="action-definition-label",
        Type="AWS::DeleteEcrRepository",
        DependsOn=['put-a-label-here'],
        Params=ActionParams(
            Account="The account to use for the action (required)",
            Region="The region to create the stack in (required)",
            RepositoryName="The name of the ECR repository to delete (required)",
        ),
        Scope="Based on your deployment details, it one of 'portfolio', 'app', 'branch', or 'build'",
    )

    return definition


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
            region=self.region, role=util.get_provisioning_role_arn(self.account)
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
