"""Delete an ECR repository"""

from typing import Any
from pydantic import Field, model_validator

from botocore.exceptions import ClientError

import core_logging as log

from core_framework.models import ActionSpec, ActionParams, DeploymentDetails

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


class DeleteEcrRepositoryActionParams(ActionParams):
    """
    Parameters for the DeleteEcrRepositoryAction.

    :param account: The account to use for the action (required)
    :type account: str
    :param region: The region where the ECR repository is located (required)
    :type region: str
    :param repository_name: The name of the ECR repository to delete (required)
    :type repository_name: str
    """

    repository_name: str = Field(
        ...,
        alias="RepositoryName",
        description="The name of the ECR repository to delete (required)",
    )


class DeleteEcrRepositoryActionSpec(ActionSpec):
    """
    Generate the action definition for DeleteEcrRepositoryAction.

    This class provides default values and validation for DeleteEcrRepositoryAction parameters.

    :param values: Dictionary of action specification values
    :type values: dict[str, Any]
    :return: Validated action specification values
    :rtype: dict[str, Any]
    """

    @model_validator(mode="before")
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """
        Generate the action definition for DeleteEcrRepositoryAction.

        This class provides default values and validation for DeleteEcrRepositoryAction parameters.

        :param values: Dictionary of action specification values
        :type values: dict[str, Any]
        :return: Validated action specification values
        :rtype: dict[str, Any]
        """
        if not (values.get("name") or values.get("Name")):
            values["name"] = "action-aws-deleteecrrepository-name"
        if not (values.get("kind") or values.get("Kind")):
            values["kind"] = "AWS::DeleteEcrRepository"
        if not values.get("depends_on", values.get("DependsOn")):  # arrays are falsy if empty
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Params")):
            values["params"] = {
                "account": "",
                "region": "",
                "repository_name": "",
            }
        return values


class DeleteEcrRepositoryAction(BaseAction):
    """
    Delete an ECR repository.

    This action will delete an ECR repository including all images within it.
    The action handles both existing and non-existing repositories gracefully.

    :param definition: The action specification containing configuration details
    :type definition: ActionSpec
    :param context: The Jinja2 rendering context containing all variables
    :type context: dict[str, Any]
    :param deployment_details: Client/portfolio/app/branch/build information
    :type deployment_details: DeploymentDetails

    .. rubric:: Parameters

    :Name: Enter a name to define this action instance
    :Kind: Use the value ``AWS::DeleteEcrRepository``
    :Params.Account: The account where the ECR repository is located (required)
    :Params.Region: The region where the ECR repository is located (required)
    :Params.RepositoryName: The name of the ECR repository to delete (required)

    .. rubric:: ActionSpec Example

    .. code-block:: yaml

        - Name: action-aws-deleteecrrepository-name
          Kind: "AWS::DeleteEcrRepository"
          Params:
            Account: "154798051514"
            Region: "ap-southeast-1"
            RepositoryName: "my-ecr-repository"
          Scope: "build"

    .. note::
        The action uses ``force=True`` to delete repositories containing images.

    .. warning::
        Repository deletion is irreversible and will delete all contained images.
    """

    def __init__(
        self,
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        # Validate the action parameters
        self.params = DeleteEcrRepositoryActionParams(**definition.params)

    def _resolve(self):
        """
        Resolve template variables in action parameters.

        This method renders Jinja2 templates in the action parameters using the current context.
        """
        log.trace("Resolving DeleteEcrRepositoryAction")

        self.params.account = self.renderer.render_string(self.params.account, self.context)
        self.params.region = self.renderer.render_string(self.params.region, self.context)
        self.params.repository_name = self.renderer.render_string(self.params.repository_name, self.context)

        log.trace("DeleteEcrRepositoryAction resolved")

    def _execute(self):
        """
        Execute the ECR repository deletion operation.

        This method deletes the specified ECR repository and sets appropriate
        state outputs for tracking.

        :raises: Sets action to failed if repository name is missing or ECR operation fails
        """
        log.trace("Executing DeleteEcrRepositoryAction")

        # Validate required parameters
        if not self.params.repository_name or self.params.repository_name == "":
            self.set_failed("RepositoryName parameter is required")
            log.error("RepositoryName parameter is required")
            return

        # Set initial state information
        self.set_state("RepositoryName", self.params.repository_name)
        self.set_state("Region", self.params.region)
        self.set_state("Account", self.params.account)
        self.set_state("DeletionStarted", True)
        self.set_state("StartTime", util.get_current_timestamp())

        # Set outputs for other actions to reference
        self.set_output("RepositoryName", self.params.repository_name)
        self.set_output("Region", self.params.region)
        self.set_output("DeletionStarted", True)

        # Obtain an ECR client
        try:
            ecr_client = aws.ecr_client(
                region=self.params.region,
                role=util.get_provisioning_role_arn(self.params.account),
            )
        except Exception as e:
            log.error("Failed to create ECR client: {}", e)
            self.set_failed(f"Failed to create ECR client: {e}")
            return

        # Check if repository exists first for better logging
        repository_exists = False
        try:
            describe_response = ecr_client.describe_repositories(
                registryId=self.params.account,
                repositoryNames=[self.params.repository_name],
            )

            if describe_response.get("repositories"):
                repository_info = describe_response["repositories"][0]
                repository_exists = True

                # Store repository information before deletion
                self.set_state("RepositoryUri", repository_info.get("repositoryUri", ""))
                self.set_state("ImageCount", repository_info.get("imageCount", 0))
                self.set_state("RepositorySize", repository_info.get("repositorySizeInBytes", 0))
                self.set_state(
                    "CreatedAt",
                    (repository_info.get("createdAt", "").isoformat() if repository_info.get("createdAt") else ""),
                )

                log.debug(
                    "Repository '{}' exists with {} images ({} bytes)",
                    self.params.repository_name,
                    repository_info.get("imageCount", 0),
                    repository_info.get("repositorySizeInBytes", 0),
                )

        except ClientError as e:
            if e.response["Error"]["Code"] == "RepositoryNotFoundException":
                repository_exists = False
                log.debug("Repository '{}' does not exist", self.params.repository_name)
            else:
                log.error(
                    "Error checking repository '{}': {}",
                    self.params.repository_name,
                    e.response["Error"]["Message"],
                )
                self.set_failed(f"Failed to check repository '{self.params.repository_name}': {e.response['Error']['Message']}")
                return
        except Exception as e:
            log.error(
                "Unexpected error checking repository '{}': {}",
                self.params.repository_name,
                e,
            )
            self.set_failed(f"Unexpected error checking repository '{self.params.repository_name}': {e}")
            return

        self.set_state("RepositoryExisted", repository_exists)

        # Attempt to delete the repository
        if repository_exists:
            self.set_running(f"Deleting ECR repository '{self.params.repository_name}'")

            try:
                ecr_client.delete_repository(
                    registryId=self.params.account,
                    repositoryName=self.params.repository_name,
                    force=True,  # Delete even if it contains images
                )

                # Set comprehensive state outputs
                self.set_state("DeletionCompleted", True)
                self.set_state("CompletionTime", util.get_current_timestamp())
                self.set_state("DeletionResult", "SUCCESS")

                # Set outputs for other actions
                self.set_output("DeletionCompleted", True)
                self.set_output("DeletionResult", "SUCCESS")

                self.set_complete(f"ECR repository '{self.params.repository_name}' has been deleted successfully")
                log.debug(
                    "Successfully deleted ECR repository '{}'",
                    self.params.repository_name,
                )

            except ClientError as e:
                error_code = e.response["Error"]["Code"]
                error_message = e.response["Error"]["Message"]

                if error_code == "RepositoryNotFoundException":
                    # Repository was deleted between our check and delete call
                    log.warning(
                        "ECR repository '{}' was not found during deletion (may have been deleted concurrently)",
                        self.params.repository_name,
                    )
                    self.set_state("DeletionCompleted", True)
                    self.set_state("CompletionTime", util.get_current_timestamp())
                    self.set_state("DeletionResult", "ALREADY_DELETED")
                    self.set_output("DeletionCompleted", True)
                    self.set_output("DeletionResult", "ALREADY_DELETED")
                    self.set_complete(f"ECR repository '{self.params.repository_name}' was already deleted")
                elif error_code == "RepositoryNotEmptyException":
                    # This shouldn't happen with force=True, but handle gracefully
                    log.error(
                        "ECR repository '{}' could not be deleted - repository not empty: {}",
                        self.params.repository_name,
                        error_message,
                    )
                    self.set_state("DeletionResult", "FAILED_NOT_EMPTY")
                    self.set_failed(
                        f"Repository '{self.params.repository_name}' could not be deleted - repository not empty: {error_message}"
                    )
                else:
                    log.error(
                        "Error deleting ECR repository '{}': {} - {}",
                        self.params.repository_name,
                        error_code,
                        error_message,
                    )
                    self.set_state("DeletionResult", "FAILED")
                    self.set_state("FailureReason", f"{error_code}: {error_message}")
                    self.set_failed(f"Failed to delete repository '{self.params.repository_name}': {error_message}")

            except Exception as e:
                log.error(
                    "Unexpected error deleting ECR repository '{}': {}",
                    self.params.repository_name,
                    e,
                )
                self.set_state("DeletionResult", "FAILED")
                self.set_state("FailureReason", str(e))
                self.set_failed(f"Unexpected error deleting repository '{self.params.repository_name}': {e}")
        else:
            # Repository doesn't exist - treat as successful deletion
            log.info(
                "ECR repository '{}' does not exist, treating as successful deletion",
                self.params.repository_name,
            )
            self.set_state("DeletionCompleted", True)
            self.set_state("CompletionTime", util.get_current_timestamp())
            self.set_state("DeletionResult", "NOT_FOUND")

            self.set_output("DeletionCompleted", True)
            self.set_output("DeletionResult", "NOT_FOUND")

            self.set_complete(f"ECR repository '{self.params.repository_name}' does not exist, may have been previously deleted")

        log.trace("DeleteEcrRepositoryAction execution completed")

    def _check(self):
        """
        Check the status of the ECR repository deletion operation.

        .. note::
            ECR repository deletion is synchronous, so this method should not be called.
        """
        log.trace("DeleteEcrRepositoryAction check")

        # ECR repository deletion is synchronous, so this shouldn't be called
        self.set_failed("Internal error - _check() should not have been called for ECR repository deletion")

        log.trace("DeleteEcrRepositoryAction check completed")

    def _unexecute(self):
        """
        Rollback the ECR repository deletion operation.

        .. note::
            ECR repository deletion cannot be undone. This method is a no-op.
        """
        log.trace("Unexecuting DeleteEcrRepositoryAction")

        # ECR repository deletion cannot be undone
        log.warning(
            "ECR repository deletion cannot be rolled back - repository '{}' remains deleted",
            self.params.repository_name,
        )

        self.set_state("RollbackAttempted", True)
        self.set_state("RollbackResult", "NOT_POSSIBLE")

        self.set_complete("ECR repository deletion cannot be rolled back")

        log.trace("DeleteEcrRepositoryAction unexecution completed")

    def _cancel(self):
        """
        Cancel the ECR repository deletion operation.

        .. note::
            ECR repository deletion is synchronous and cannot be cancelled once started.
        """
        log.trace("Cancelling DeleteEcrRepositoryAction")

        # ECR repository deletion is synchronous and cannot be cancelled
        self.set_complete("ECR repository deletion cannot be cancelled")

        log.trace("DeleteEcrRepositoryAction cancellation completed")

    @classmethod
    def generate_action_spec(cls, **kwargs) -> DeleteEcrRepositoryActionSpec:
        return DeleteEcrRepositoryActionSpec(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> DeleteEcrRepositoryActionParams:
        return DeleteEcrRepositoryActionParams(**kwargs)
