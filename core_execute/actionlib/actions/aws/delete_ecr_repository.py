"""Delete an ECR repository (handles non-existent repositories gracefully)."""

from typing import Any
from pydantic import Field, model_validator

from botocore.exceptions import ClientError

import core_logging as log

from core_framework.models import ActionResource, ActionSpec, DeploymentDetails

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


class DeleteEcrRepositoryActionSpec(ActionSpec):
    """Parameters for deleting an ECR repository.

    Attributes:
      account: AWS account ID used for the action.
      region: AWS region where the repository resides.
      repository_name: Name of the ECR repository to delete.
    """

    repository_name: str = Field(
        ...,
        alias="RepositoryName",
        description="The name of the ECR repository to delete (required)",
    )


class DeleteEcrRepositoryActionResource(ActionResource):
    """Resource model for DeleteEcrRepository (normalizes kind/spec)."""

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Normalize incoming values and enforce canonical kind/spec."""
        if not isinstance(values, dict):
            return values

        values.pop("kind", None)
        values.pop("Kind", None)
        values["kind"] = "AWS::DeleteEcrRepository"

        spec = values.pop("spec", None) or values.pop("Spec", None)
        if isinstance(spec, dict):
            values["spec"] = spec
        elif isinstance(spec, DeleteEcrRepositoryActionSpec):
            values["spec"] = spec.model_dump()

        return values


class DeleteEcrRepositoryAction(BaseAction):
    """Delete an ECR repository and all images it contains.

    Treats missing repositories as success. Records progress and results in state/outputs.
    """

    def __init__(
        self,
        definition: ActionResource,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        """Initialize the action and validate parameters.

        Args:
          definition: Action resource with metadata/spec.
          context: Rendering context for templates.
          deployment_details: Deployment metadata.

        """
        super().__init__(definition, context, deployment_details)

        # Validate the action parameters
        self.spec = DeleteEcrRepositoryActionSpec(**definition.spec)

    def _resolve(self):
        """Render template variables in account, region, and repository_name."""
        log.trace("Resolving DeleteEcrRepositoryAction")

        self.spec.account = self.renderer.render_string(self.spec.account, self.context)
        self.spec.region = self.renderer.render_string(self.spec.region, self.context)
        self.spec.repository_name = self.renderer.render_string(self.spec.repository_name, self.context)

        log.trace("DeleteEcrRepositoryAction resolved")

    def _execute(self):
        """Delete the ECR repository and set state/outputs.

        Sets failed status for missing parameters or unexpected ECR errors.
        """
        log.trace("Executing DeleteEcrRepositoryAction")

        # Validate required parameters
        if not self.spec.repository_name or self.spec.repository_name == "":
            self.set_failed("RepositoryName parameter is required")
            log.error("RepositoryName parameter is required")
            return

        # Set initial state information
        self.set_state("RepositoryName", self.spec.repository_name)
        self.set_state("Region", self.spec.region)
        self.set_state("Account", self.spec.account)
        self.set_state("DeletionStarted", True)
        self.set_state("StartTime", util.get_current_timestamp())

        # Set outputs for other actions to reference
        self.set_output("RepositoryName", self.spec.repository_name)
        self.set_output("Region", self.spec.region)
        self.set_output("DeletionStarted", True)

        # Obtain an ECR client
        try:
            ecr_client = aws.ecr_client(
                region=self.spec.region,
                role=util.get_provisioning_role_arn(self.spec.account),
            )
        except Exception as e:
            log.error("Failed to create ECR client: {}", e)
            self.set_failed(f"Failed to create ECR client: {e}")
            return

        # Check if repository exists first for better logging
        repository_exists = False
        try:
            describe_response = ecr_client.describe_repositories(
                registryId=self.spec.account,
                repositoryNames=[self.spec.repository_name],
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
                    self.spec.repository_name,
                    repository_info.get("imageCount", 0),
                    repository_info.get("repositorySizeInBytes", 0),
                )

        except ClientError as e:
            if e.response["Error"]["Code"] == "RepositoryNotFoundException":
                repository_exists = False
                log.debug("Repository '{}' does not exist", self.spec.repository_name)
            else:
                log.error(
                    "Error checking repository '{}': {}",
                    self.spec.repository_name,
                    e.response["Error"]["Message"],
                )
                self.set_failed(f"Failed to check repository '{self.spec.repository_name}': {e.response['Error']['Message']}")
                return
        except Exception as e:
            log.error(
                "Unexpected error checking repository '{}': {}",
                self.spec.repository_name,
                e,
            )
            self.set_failed(f"Unexpected error checking repository '{self.spec.repository_name}': {e}")
            return

        self.set_state("RepositoryExisted", repository_exists)

        # Attempt to delete the repository
        if repository_exists:
            self.set_running(f"Deleting ECR repository '{self.spec.repository_name}'")

            try:
                ecr_client.delete_repository(
                    registryId=self.spec.account,
                    repositoryName=self.spec.repository_name,
                    force=True,  # Delete even if it contains images
                )

                # Set comprehensive state outputs
                self.set_state("DeletionCompleted", True)
                self.set_state("CompletionTime", util.get_current_timestamp())
                self.set_state("DeletionResult", "SUCCESS")

                # Set outputs for other actions
                self.set_output("DeletionCompleted", True)
                self.set_output("DeletionResult", "SUCCESS")

                self.set_complete(f"ECR repository '{self.spec.repository_name}' has been deleted successfully")
                log.debug(
                    "Successfully deleted ECR repository '{}'",
                    self.spec.repository_name,
                )

            except ClientError as e:
                error_code = e.response["Error"]["Code"]
                error_message = e.response["Error"]["Message"]

                if error_code == "RepositoryNotFoundException":
                    # Repository was deleted between our check and delete call
                    log.warning(
                        "ECR repository '{}' was not found during deletion (may have been deleted concurrently)",
                        self.spec.repository_name,
                    )
                    self.set_state("DeletionCompleted", True)
                    self.set_state("CompletionTime", util.get_current_timestamp())
                    self.set_state("DeletionResult", "ALREADY_DELETED")
                    self.set_output("DeletionCompleted", True)
                    self.set_output("DeletionResult", "ALREADY_DELETED")
                    self.set_complete(f"ECR repository '{self.spec.repository_name}' was already deleted")
                elif error_code == "RepositoryNotEmptyException":
                    # This shouldn't happen with force=True, but handle gracefully
                    log.error(
                        "ECR repository '{}' could not be deleted - repository not empty: {}",
                        self.spec.repository_name,
                        error_message,
                    )
                    self.set_state("DeletionResult", "FAILED_NOT_EMPTY")
                    self.set_failed(
                        f"Repository '{self.spec.repository_name}' could not be deleted - repository not empty: {error_message}"
                    )
                else:
                    log.error(
                        "Error deleting ECR repository '{}': {} - {}",
                        self.spec.repository_name,
                        error_code,
                        error_message,
                    )
                    self.set_state("DeletionResult", "FAILED")
                    self.set_state("FailureReason", f"{error_code}: {error_message}")
                    self.set_failed(f"Failed to delete repository '{self.spec.repository_name}': {error_message}")

            except Exception as e:
                log.error(
                    "Unexpected error deleting ECR repository '{}': {}",
                    self.spec.repository_name,
                    e,
                )
                self.set_state("DeletionResult", "FAILED")
                self.set_state("FailureReason", str(e))
                self.set_failed(f"Unexpected error deleting repository '{self.spec.repository_name}': {e}")
        else:
            # Repository doesn't exist - treat as successful deletion
            log.info(
                "ECR repository '{}' does not exist, treating as successful deletion",
                self.spec.repository_name,
            )
            self.set_state("DeletionCompleted", True)
            self.set_state("CompletionTime", util.get_current_timestamp())
            self.set_state("DeletionResult", "NOT_FOUND")

            self.set_output("DeletionCompleted", True)
            self.set_output("DeletionResult", "NOT_FOUND")

            self.set_complete(f"ECR repository '{self.spec.repository_name}' does not exist, may have been previously deleted")

        log.trace("DeleteEcrRepositoryAction execution completed")

    def _check(self):
        """Not applicable; ECR repository deletion is synchronous."""
        log.trace("DeleteEcrRepositoryAction check")

        # ECR repository deletion is synchronous, so this shouldn't be called
        self.set_failed("Internal error - _check() should not have been called for ECR repository deletion")

        log.trace("DeleteEcrRepositoryAction check completed")

    def _unexecute(self):
        """No rollback; repository deletions are irreversible."""
        log.trace("Unexecuting DeleteEcrRepositoryAction")

        log.warning(
            "ECR repository deletion cannot be rolled back - repository '{}' remains deleted",
            self.spec.repository_name,
        )

        self.set_state("RollbackAttempted", True)
        self.set_state("RollbackResult", "NOT_POSSIBLE")

        self.set_complete("ECR repository deletion cannot be rolled back")

        log.trace("DeleteEcrRepositoryAction unexecution completed")

    def _cancel(self):
        """No-op; deletion is synchronous and cannot be cancelled."""
        log.trace("Cancelling DeleteEcrRepositoryAction")

        self.set_complete("ECR repository deletion cannot be cancelled")

        log.trace("DeleteEcrRepositoryAction cancellation completed")

    @classmethod
    def generate_action_resource(cls, **kwargs) -> DeleteEcrRepositoryActionResource:
        """Factory: create a typed DeleteEcrRepositoryActionResource."""
        return DeleteEcrRepositoryActionResource(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> DeleteEcrRepositoryActionSpec:
        """Factory: create typed DeleteEcrRepositoryActionSpec."""
