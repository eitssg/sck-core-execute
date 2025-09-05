from typing import Any
from pydantic import Field, model_validator
from botocore.exceptions import ClientError

import core_logging as log
import core_framework as util

from core_framework.models import ActionResource, ActionSpec, DeploymentDetails
from core_execute.actionlib.action import BaseAction
import core_helper.aws as aws


class DeleteChangeSetActionSpec(ActionSpec):
    """Parameters for deleting a CloudFormation change set.

    Attributes:
      account: AWS account ID to use.
      region: AWS region of the stack.
      stack_name: Name of the stack that owns the change set.
      change_set_name: Name of the change set to delete.
    """

    stack_name: str = Field(
        ...,
        alias="StackName",
        description="The name of the stack containing the change set",
    )
    change_set_name: str = Field(..., alias="ChangeSetName", description="The name of the change set to delete")


class DeleteChangeSetActionResource(ActionResource):
    """Resource model for DeleteChangeSet (normalizes kind/spec)."""

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Normalize incoming values and enforce canonical kind/spec."""
        if not isinstance(values, dict):
            return values

        values.pop("kind", None)
        values.pop("Kind", None)
        values["kind"] = "AWS::DeleteChangeSet"

        spec = values.pop("spec", None) or values.pop("Spec", None)
        if isinstance(spec, dict):
            values["spec"] = spec
        elif isinstance(spec, DeleteChangeSetActionSpec):
            values["spec"] = spec.model_dump()

        return values


class DeleteChangeSetAction(BaseAction[DeleteChangeSetActionSpec]):
    """Delete a CloudFormation change set without modifying the stack.

    - Supports cross-account deletion via role assumption
    - Synchronous operation; no polling required
    - Records inputs/results in action state and outputs
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

        self.spec = DeleteChangeSetActionSpec(**definition.spec)

    def _resolve(self):
        """Render templates in account, region, stack_name, and change_set_name."""
        log.trace("Resolving DeleteChangeSetAction")

        self.spec.account = self.renderer.render_string(self.spec.account, self.context)
        self.spec.region = self.renderer.render_string(self.spec.region, self.context)
        self.spec.stack_name = self.renderer.render_string(self.spec.stack_name, self.context)
        self.spec.change_set_name = self.renderer.render_string(self.spec.change_set_name, self.context)

        log.trace("DeleteChangeSetAction resolved")

    def _execute(self):
        """Delete the specified CloudFormation change set and set results.

        Sets:
          - State: ChangeSetExists, DeletionCompleted, DeletionResult, timestamps
          - Outputs: ChangeSetName, StackName, Region, DeletionCompleted, DeletionResult

        Raises:
          Sets failed status when required parameters are missing or on unexpected AWS errors.
        """
        log.trace("Executing DeleteChangeSetAction")

        # Validate required parameters
        if not self.spec.stack_name or self.spec.stack_name == "":
            self.set_failed("StackName parameter is required")
            log.error("StackName parameter is required")
            return

        if not self.spec.change_set_name or self.spec.change_set_name == "":
            self.set_failed("ChangeSetName parameter is required")
            log.error("ChangeSetName parameter is required")
            return

        # Set initial state information
        self.set_state("ChangeSetName", self.spec.change_set_name)
        self.set_state("StackName", self.spec.stack_name)
        self.set_state("Region", self.spec.region)
        self.set_state("Account", self.spec.account)
        self.set_state("ChangeSetDeletionStarted", True)
        self.set_state("StartTime", util.get_current_timestamp())

        # Set outputs for other actions to reference
        self.set_output("ChangeSetName", self.spec.change_set_name)
        self.set_output("StackName", self.spec.stack_name)
        self.set_output("Region", self.spec.region)

        # Obtain a CloudFormation client
        try:
            cfn_client = aws.cfn_client(
                region=self.spec.region,
                role=util.get_provisioning_role_arn(self.spec.account),
            )
        except Exception as e:
            log.error("Failed to create CloudFormation client: {}", e)
            self.set_failed(f"Failed to create CloudFormation client: {e}")
            return

        # Check if change set exists first
        change_set_exists = False
        try:
            response = cfn_client.describe_change_set(
                StackName=self.spec.stack_name,
                ChangeSetName=self.spec.change_set_name,
            )

            change_set_exists = True
            change_set_status = response.get("Status", "UNKNOWN")
            change_set_arn = response.get("Id", "")

            log.debug(
                "Change set '{}' exists with status: {}",
                self.spec.change_set_name,
                change_set_status,
            )

            # Store change set information
            self.set_state("ChangeSetExists", True)
            self.set_state("ChangeSetArn", change_set_arn)
            self.set_state("ChangeSetStatus", change_set_status)

        except ClientError as e:
            error_code = e.response["Error"]["Code"]

            if error_code == "ChangeSetNotFoundException":
                change_set_exists = False
                log.debug(
                    "Change set '{}' does not exist for stack '{}'",
                    self.spec.change_set_name,
                    self.spec.stack_name,
                )
            else:
                error_message = e.response["Error"]["Message"]
                log.error(
                    "Error checking change set '{}': {} - {}",
                    self.spec.change_set_name,
                    error_code,
                    error_message,
                )
                self.set_failed(f"Failed to check change set '{self.spec.change_set_name}': {error_message}")
                return

        except Exception as e:
            log.error(
                "Unexpected error checking change set '{}': {}",
                self.spec.change_set_name,
                e,
            )
            self.set_failed(f"Unexpected error checking change set '{self.spec.change_set_name}': {e}")
            return

        self.set_state("ChangeSetExists", change_set_exists)

        # Perform the deletion
        if change_set_exists:
            try:
                log.info(
                    "Deleting change set '{}' from stack '{}'",
                    self.spec.change_set_name,
                    self.spec.stack_name,
                )

                cfn_client.delete_change_set(
                    StackName=self.spec.stack_name,
                    ChangeSetName=self.spec.change_set_name,
                )

                # Set successful deletion state
                self.set_state("DeletionCompleted", True)
                self.set_state("CompletionTime", util.get_current_timestamp())
                self.set_state("DeletionResult", "SUCCESS")

                # Set outputs
                self.set_output("DeletionCompleted", True)
                self.set_output("DeletionResult", "SUCCESS")

                self.set_complete(
                    f"Change set '{self.spec.change_set_name}' deleted successfully from stack '{self.spec.stack_name}'"
                )
                log.info(
                    "Change set '{}' deleted successfully from stack '{}'",
                    self.spec.change_set_name,
                    self.spec.stack_name,
                )

            except ClientError as e:
                error_code = e.response["Error"]["Code"]
                error_message = e.response["Error"]["Message"]

                if error_code == "ChangeSetNotFoundException":
                    # Change set was already deleted (race condition)
                    log.info(
                        "Change set '{}' was already deleted from stack '{}'",
                        self.spec.change_set_name,
                        self.spec.stack_name,
                    )
                    self.set_state("DeletionCompleted", True)
                    self.set_state("CompletionTime", util.get_current_timestamp())
                    self.set_state("DeletionResult", "ALREADY_DELETED")

                    self.set_output("DeletionCompleted", True)
                    self.set_output("DeletionResult", "ALREADY_DELETED")

                    self.set_complete(
                        f"Change set '{self.spec.change_set_name}' was already deleted from stack '{self.spec.stack_name}'"
                    )
                else:
                    log.error(
                        "Error deleting change set '{}': {} - {}",
                        self.spec.change_set_name,
                        error_code,
                        error_message,
                    )
                    self.set_state("DeletionResult", "FAILED")
                    self.set_state("FailureReason", f"{error_code}: {error_message}")
                    self.set_failed(f"Failed to delete change set '{self.spec.change_set_name}': {error_message}")

            except Exception as e:
                log.error(
                    "Unexpected error deleting change set '{}': {}",
                    self.spec.change_set_name,
                    e,
                )
                self.set_state("DeletionResult", "FAILED")
                self.set_state("FailureReason", str(e))
                self.set_failed(f"Unexpected error deleting change set '{self.spec.change_set_name}': {e}")
        else:
            # Change set doesn't exist - treat as successful deletion
            log.info(
                "Change set '{}' does not exist for stack '{}', treating as successful deletion",
                self.spec.change_set_name,
                self.spec.stack_name,
            )
            self.set_state("DeletionCompleted", True)
            self.set_state("CompletionTime", util.get_current_timestamp())
            self.set_state("DeletionResult", "NOT_FOUND")

            self.set_output("DeletionCompleted", True)
            self.set_output("DeletionResult", "NOT_FOUND")

            self.set_complete(
                f"Change set '{self.spec.change_set_name}' does not exist for stack '{self.spec.stack_name}', may have been previously deleted"
            )

        log.trace("DeleteChangeSetAction execution completed")

    def _check(self):
        """Not applicable; change set deletion is synchronous."""
        log.trace("DeleteChangeSetAction check")

        # Change set deletion is synchronous, so this shouldn't be called
        self.set_failed("Internal error - _check() should not have been called for change set deletion")

    def _unexecute(self):
        """No rollback; change set deletion cannot be undone."""
        log.trace("Unexecuting DeleteChangeSetAction")

        log.info(
            "Change set deletion cannot be rolled back - change set '{}' would need to be recreated",
            self.spec.change_set_name,
        )

        self.set_state("RollbackResult", "NOT_POSSIBLE")
        self.set_complete(f"Change set deletion cannot be rolled back - '{self.spec.change_set_name}' would need to be recreated")

        log.trace("DeleteChangeSetAction unexecution completed")

    def _cancel(self):
        """No-op; deletion is synchronous and cannot be cancelled."""
        log.trace("Cancelling DeleteChangeSetAction")

        log.info("Change set deletion is synchronous and cannot be cancelled")
        self.set_complete("Change set deletion is synchronous and cannot be cancelled")

        log.trace("DeleteChangeSetAction cancellation completed")

    @classmethod
    def generate_action_resource(cls, **kwargs) -> DeleteChangeSetActionResource:
        """Factory: create a typed DeleteChangeSetActionResource."""
        return DeleteChangeSetActionResource(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> DeleteChangeSetActionSpec:
        """Factory: create a typed DeleteChangeSetActionSpec."""
        return DeleteChangeSetActionSpec(**kwargs)
