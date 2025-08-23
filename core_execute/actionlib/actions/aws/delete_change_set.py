from typing import Any
from pydantic import Field, model_validator
from botocore.exceptions import ClientError

import core_logging as log
import core_framework as util

from core_framework.models import ActionSpec, ActionParams, DeploymentDetails
from core_execute.actionlib.action import BaseAction
import core_helper.aws as aws


class DeleteChangeSetActionParams(ActionParams):
    """Parameters for the DeleteChangeSetAction

    This class defines the parameters that can be used in the action.
    """

    stack_name: str = Field(
        ...,
        alias="StackName",
        description="The name of the stack containing the change set",
    )
    change_set_name: str = Field(
        ..., alias="ChangeSetName", description="The name of the change set to delete"
    )


class DeleteChangeSetActionSpec(ActionSpec):
    """Generate the action definition"""

    @model_validator(mode="before")
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Validate the parameters for the DeleteChangeSetActionSpec"""
        if not (values.get("name") or values.get("Name")):
            values["name"] = "action-aws-deletechangeset-name"
        if not (values.get("kind") or values.get("Kind")):
            values["kind"] = "AWS::DeleteChangeSet"
        if not values.get(
            "depends_on", values.get("DependsOn")
        ):  # arrays are falsy if empty
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Params")):
            values["params"] = {
                "account": "",
                "region": "",
                "stack_name": "",
                "change_set_name": "",
            }
        return values


class DeleteChangeSetAction(BaseAction):
    """CloudFormation Change Set Deletion Action

    This action deletes a CloudFormation change set without affecting the underlying stack.
    It supports cross-account deployments via role assumption and handles
    step function execution patterns with state persistence.

    Kind: Use the value: ``AWS::DeleteChangeSet``

    .. rubric: ActionSpec:

    .. tip:: s3:/<bucket>/artifacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Name: action-aws-deletechangeset-name
              Kind: "AWS::DeleteChangeSet"
              Params:
                Account: "154798051514"
                Region: "ap-southeast-1"
                StackName: "my-stack"
                ChangeSetName: "my-changeset"
              Scope: "portfolio"

    State Variables:
        - ChangeSetDeletionStarted: Timestamp when deletion began
        - ChangeSetName: Name of the change set being deleted
        - StackName: Name of the target stack
        - ChangeSetExists: Whether the change set existed before deletion
        - DeletionResult: Result of the deletion operation

    Output Variables:
        - ChangeSetName: Name of the deleted change set
        - StackName: Name of the target stack
        - DeletionCompleted: Whether deletion was completed
        - DeletionResult: Result of the deletion operation
    """

    def __init__(
        self,
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        self.params = DeleteChangeSetActionParams(**definition.params)

    def _resolve(self):
        """
        Resolve template variables in action parameters.

        This method renders Jinja2 templates in the action parameters using the current context.
        """
        log.trace("Resolving DeleteChangeSetAction")

        self.params.account = self.renderer.render_string(
            self.params.account, self.context
        )
        self.params.region = self.renderer.render_string(
            self.params.region, self.context
        )
        self.params.stack_name = self.renderer.render_string(
            self.params.stack_name, self.context
        )
        self.params.change_set_name = self.renderer.render_string(
            self.params.change_set_name, self.context
        )

        log.trace("DeleteChangeSetAction resolved")

    def _execute(self):
        """
        Execute the CloudFormation change set deletion operation.

        This method deletes the specified CloudFormation change set and sets appropriate
        state outputs for tracking. The operation is synchronous, so no _check() is needed.

        :raises: Sets action to failed if parameters are missing or CloudFormation operation fails
        """
        log.trace("Executing DeleteChangeSetAction")

        # Validate required parameters
        if not self.params.stack_name or self.params.stack_name == "":
            self.set_failed("StackName parameter is required")
            log.error("StackName parameter is required")
            return

        if not self.params.change_set_name or self.params.change_set_name == "":
            self.set_failed("ChangeSetName parameter is required")
            log.error("ChangeSetName parameter is required")
            return

        # Set initial state information
        self.set_state("ChangeSetName", self.params.change_set_name)
        self.set_state("StackName", self.params.stack_name)
        self.set_state("Region", self.params.region)
        self.set_state("Account", self.params.account)
        self.set_state("ChangeSetDeletionStarted", True)
        self.set_state("StartTime", util.get_current_timestamp())

        # Set outputs for other actions to reference
        self.set_output("ChangeSetName", self.params.change_set_name)
        self.set_output("StackName", self.params.stack_name)
        self.set_output("Region", self.params.region)

        # Obtain a CloudFormation client
        try:
            cfn_client = aws.cfn_client(
                region=self.params.region,
                role=util.get_provisioning_role_arn(self.params.account),
            )
        except Exception as e:
            log.error("Failed to create CloudFormation client: {}", e)
            self.set_failed(f"Failed to create CloudFormation client: {e}")
            return

        # Check if change set exists first
        change_set_exists = False
        try:
            response = cfn_client.describe_change_set(
                StackName=self.params.stack_name,
                ChangeSetName=self.params.change_set_name,
            )

            change_set_exists = True
            change_set_status = response.get("Status", "UNKNOWN")
            change_set_arn = response.get("Id", "")

            log.debug(
                "Change set '{}' exists with status: {}",
                self.params.change_set_name,
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
                    self.params.change_set_name,
                    self.params.stack_name,
                )
            else:
                error_message = e.response["Error"]["Message"]
                log.error(
                    "Error checking change set '{}': {} - {}",
                    self.params.change_set_name,
                    error_code,
                    error_message,
                )
                self.set_failed(
                    f"Failed to check change set '{self.params.change_set_name}': {error_message}"
                )
                return

        except Exception as e:
            log.error(
                "Unexpected error checking change set '{}': {}",
                self.params.change_set_name,
                e,
            )
            self.set_failed(
                f"Unexpected error checking change set '{self.params.change_set_name}': {e}"
            )
            return

        self.set_state("ChangeSetExists", change_set_exists)

        # Perform the deletion
        if change_set_exists:
            try:
                log.info(
                    "Deleting change set '{}' from stack '{}'",
                    self.params.change_set_name,
                    self.params.stack_name,
                )

                cfn_client.delete_change_set(
                    StackName=self.params.stack_name,
                    ChangeSetName=self.params.change_set_name,
                )

                # Set successful deletion state
                self.set_state("DeletionCompleted", True)
                self.set_state("CompletionTime", util.get_current_timestamp())
                self.set_state("DeletionResult", "SUCCESS")

                # Set outputs
                self.set_output("DeletionCompleted", True)
                self.set_output("DeletionResult", "SUCCESS")

                self.set_complete(
                    f"Change set '{self.params.change_set_name}' deleted successfully from stack '{self.params.stack_name}'"
                )
                log.info(
                    "Change set '{}' deleted successfully from stack '{}'",
                    self.params.change_set_name,
                    self.params.stack_name,
                )

            except ClientError as e:
                error_code = e.response["Error"]["Code"]
                error_message = e.response["Error"]["Message"]

                if error_code == "ChangeSetNotFoundException":
                    # Change set was already deleted (race condition)
                    log.info(
                        "Change set '{}' was already deleted from stack '{}'",
                        self.params.change_set_name,
                        self.params.stack_name,
                    )
                    self.set_state("DeletionCompleted", True)
                    self.set_state("CompletionTime", util.get_current_timestamp())
                    self.set_state("DeletionResult", "ALREADY_DELETED")

                    self.set_output("DeletionCompleted", True)
                    self.set_output("DeletionResult", "ALREADY_DELETED")

                    self.set_complete(
                        f"Change set '{self.params.change_set_name}' was already deleted from stack '{self.params.stack_name}'"
                    )
                else:
                    log.error(
                        "Error deleting change set '{}': {} - {}",
                        self.params.change_set_name,
                        error_code,
                        error_message,
                    )
                    self.set_state("DeletionResult", "FAILED")
                    self.set_state("FailureReason", f"{error_code}: {error_message}")
                    self.set_failed(
                        f"Failed to delete change set '{self.params.change_set_name}': {error_message}"
                    )

            except Exception as e:
                log.error(
                    "Unexpected error deleting change set '{}': {}",
                    self.params.change_set_name,
                    e,
                )
                self.set_state("DeletionResult", "FAILED")
                self.set_state("FailureReason", str(e))
                self.set_failed(
                    f"Unexpected error deleting change set '{self.params.change_set_name}': {e}"
                )
        else:
            # Change set doesn't exist - treat as successful deletion
            log.info(
                "Change set '{}' does not exist for stack '{}', treating as successful deletion",
                self.params.change_set_name,
                self.params.stack_name,
            )
            self.set_state("DeletionCompleted", True)
            self.set_state("CompletionTime", util.get_current_timestamp())
            self.set_state("DeletionResult", "NOT_FOUND")

            self.set_output("DeletionCompleted", True)
            self.set_output("DeletionResult", "NOT_FOUND")

            self.set_complete(
                f"Change set '{self.params.change_set_name}' does not exist for stack '{self.params.stack_name}', may have been previously deleted"
            )

        log.trace("DeleteChangeSetAction execution completed")

    def _check(self):
        """
        Check the status of the change set deletion operation.

        .. note::
            Change set deletion is synchronous, so this method should not be called.
        """
        log.trace("DeleteChangeSetAction check")

        # Change set deletion is synchronous, so this shouldn't be called
        self.set_failed(
            "Internal error - _check() should not have been called for change set deletion"
        )

    def _unexecute(self):
        """
        Rollback the change set deletion operation.

        .. note::
            Change set deletion cannot be rolled back. Once a change set is deleted,
            it would need to be recreated from scratch.
        """
        log.trace("Unexecuting DeleteChangeSetAction")

        log.info(
            "Change set deletion cannot be rolled back - change set '{}' would need to be recreated",
            self.params.change_set_name,
        )

        self.set_state("RollbackResult", "NOT_POSSIBLE")
        self.set_complete(
            f"Change set deletion cannot be rolled back - '{self.params.change_set_name}' would need to be recreated"
        )

        log.trace("DeleteChangeSetAction unexecution completed")

    def _cancel(self):
        """
        Cancel the change set deletion operation.

        .. note::
            Change set deletion is synchronous and cannot be cancelled once initiated.
        """
        log.trace("Cancelling DeleteChangeSetAction")

        log.info("Change set deletion is synchronous and cannot be cancelled")
        self.set_complete("Change set deletion is synchronous and cannot be cancelled")

        log.trace("DeleteChangeSetAction cancellation completed")

    @classmethod
    def generate_action_spec(cls, **kwargs) -> DeleteChangeSetActionSpec:
        return DeleteChangeSetActionSpec(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> DeleteChangeSetActionParams:
        return DeleteChangeSetActionParams(**kwargs)
