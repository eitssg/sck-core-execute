"""Delete an ECR repository"""

from typing import Any

from botocore.exceptions import ClientError

import core_logging as log

from core_framework.models import ActionDefinition, DeploymentDetails, ActionParams

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


def generate_template() -> ActionDefinition:
    """Generate the action definition"""

    definition = ActionDefinition(
        Label="action-definition-label",
        Type="AWS::DeleteEcrRepository",
        DependsOn=["put-a-label-here"],
        Params=ActionParams(
            Account="The account to use for the action (required)",
            Region="The region to create the stack in (required)",
            RepositoryName="The name of the ECR repository to delete (required)",
        ),
        Scope="Based on your deployment details, it one of 'portfolio', 'app', 'branch', or 'build'",
    )

    return definition


class DeleteEcrRepositoryAction(BaseAction):
    """Delete an ECR repository

    This action will delete an ECR repository.  The action will wait for the deletion to complete before returning.

    Attributes:
        Type: Use the value: ``AWS::DeleteEcrRepository``
        Params.Account: The account where the ECR repository is located
        Params.Region: The region where the ECR repository is located
        Params.RepositoryName: The name of the ECR repository to delete (required)

    .. rubric: ActionDefinition:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Label: action-aws-deleteecrrepository-label
              Type: "AWS::DeleteEcrRepository"
              Params:
                Account: "154798051514"
                Region: "ap-southeast-1"
                RepositoryName: "my-ecr-repository"
              Scope: "build"

    """

    def __init__(
        self,
        definition: ActionDefinition,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

    def _execute(self):

        log.trace("DeleteEcrRepositoryAction._execute()")

        ecr_client = aws.ecr_client(
            region=self.params.Region,
            role=util.get_provisioning_role_arn(self.params.Account),
        )

        try:
            ecr_client.delete_repository(
                registryId=self.params.Account,
                repositoryName=self.params.RepositoryName,
                force=True,
            )
            self.set_complete(
                "ECR repository '{}' has been deleted".format(
                    self.params.RepositoryName
                )
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "RepositoryNotFoundException":
                log.warning(
                    "ECR repository '{}' does not exist", self.params.RepositoryName
                )
                self.set_complete(
                    "ECR repository '{}' does not exist, may have been previously deleted".format(
                        self.params.RepositoryName
                    )
                )
            else:
                log.error(
                    "Error deleting ECR repository '{}': {}",
                    self.params.RepositoryName,
                    e,
                )
                raise

        log.trace("DeleteEcrRepositoryAction._execute() complete")

    def _check(self):
        log.trace("DeleteEcrRepositoryAction._check()")

        self.set_failed("Internal error - _check() should not have been called")

        log.trace("DeleteEcrRepositoryAction._check() complete")

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):

        log.trace("DeleteEcrRepositoryAction._resolve()")

        self.params.Account = self.renderer.render_string(
            self.params.Account, self.context
        )
        self.params.Region = self.renderer.render_string(
            self.params.Region, self.context
        )
        self.params.RepositoryName = self.renderer.render_string(
            self.params.RepositoryName, self.context
        )

        log.trace("DeleteEcrRepositoryAction._resolve() complete")
