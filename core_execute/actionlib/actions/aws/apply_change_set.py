"""Apply a CloudFormation change set and monitor the stack update.

Creates a CFN client, validates the change set, executes it, and tracks progress.
Captures stack outputs and affected resources in state/outputs.
"""

from typing import Any
from pydantic import Field, model_validator
from botocore.exceptions import ClientError

import core_logging as log
import core_framework as util

from core_framework.models import ActionResource, ActionSpec, DeploymentDetails
from core_execute.actionlib.action import BaseAction
import core_helper.aws as aws


class ApplyChangeSetActionSpec(ActionSpec):
    """Parameters for applying a CloudFormation change set.

    Attributes:
      account: AWS account ID used for the action.
      region: AWS region of the stack.
      stack_name: Target stack name.
      change_set_name: Name of the change set to execute.
    """

    stack_name: str = Field(
        ...,
        alias="StackName",
        description="The name of the stack to apply the change set",
    )

    change_set_name: str = Field(..., alias="ChangeSetName", description="The name of the change set to apply")


class ApplyChangeSetActionResource(ActionResource):
    """Resource model for ApplyChangeSetAction (normalizes kind/spec)."""

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Normalize incoming values and enforce canonical kind/spec."""
        if not isinstance(values, dict):
            return values

        values.pop("kind", None)
        values.pop("Kind", None)
        values["kind"] = "AWS::ApplyChangeSet"

        spec = values.pop("spec", None) or values.pop("Spec", None)
        if isinstance(spec, dict):
            values["spec"] = spec
        elif isinstance(spec, ApplyChangeSetActionSpec):
            values["spec"] = spec.model_dump()

        return values


class ApplyChangeSetAction(BaseAction[ApplyChangeSetActionSpec]):
    """Apply a CloudFormation change set to a stack.

    - Supports cross-account execution via role assumption
    - Persists identifiers, status, and results in state/outputs

    State (subset):
      ChangeSetApplicationStarted, ChangeSetName, StackName, StackId,
      StackStatus, ApplicationResult

    Outputs (subset):
      StackArn, StackId, StackStatus, ResourcesCreated, ResourcesUpdated,
      ResourcesDeleted, StackOutputs
    """

    def __init__(
        self,
        definition: ActionResource,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        """Initialize the action and validate parameters."""
        super().__init__(definition, context, deployment_details)

        self.spec = ApplyChangeSetActionSpec(**definition.spec)

    def _resolve(self):
        """Render templates for account, region, stack_name, and change_set_name."""
        log.trace("Resolving ApplyChangeSetAction")

        self.spec.account = self.renderer.render_string(self.spec.account, self.context)
        self.spec.region = self.renderer.render_string(self.spec.region, self.context)
        self.spec.stack_name = self.renderer.render_string(self.spec.stack_name, self.context)
        self.spec.change_set_name = self.renderer.render_string(self.spec.change_set_name, self.context)

        log.trace("ApplyChangeSetAction resolved")

    def _execute(self):
        """Execute the specified change set and set initial state/outputs.

        Sets failed status when parameters are missing or CFN operations fail.
        """
        log.trace("Executing ApplyChangeSetAction")

        # Validate required parameters
        if not self.spec.stack_name or self.spec.stack_name == "":
            self.set_failed("StackName parameter is required")
            log.error("StackName parameter is required")
            return

        if not self.spec.change_set_name or self.spec.change_set_name == "":
            self.set_failed("ChangeSetName parameter is required")
            log.error("ChangeSetName parameter is required")
            return

        # Check if change set application already started (idempotent execution)
        if self.get_state("ChangeSetApplicationStarted") and self.get_state("StackId"):
            log.info(
                "Change set application already in progress for {}",
                self.spec.change_set_name,
            )
            self.set_running(f"Change set application already in progress for {self.spec.change_set_name}")
            return

        # Set initial state information
        self.set_state("ChangeSetName", self.spec.change_set_name)
        self.set_state("StackName", self.spec.stack_name)
        self.set_state("Region", self.spec.region)
        self.set_state("Account", self.spec.account)
        self.set_state("ChangeSetApplicationStarted", True)
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

        # Verify change set exists and get its details
        try:
            change_set_response = cfn_client.describe_change_set(
                StackName=self.spec.stack_name,
                ChangeSetName=self.spec.change_set_name,
            )

            if change_set_response["Status"] != "CREATE_COMPLETE":
                log.error(
                    "Change set '{}' is not in CREATE_COMPLETE status: {}",
                    self.spec.change_set_name,
                    change_set_response["Status"],
                )
                self.set_failed(
                    f"Change set '{self.spec.change_set_name}' is not ready for execution: {change_set_response['Status']}"
                )
                return

            # Store change set information
            self.set_state("ChangeSetArn", change_set_response["Id"])
            self.set_state("ChangeSetStatus", change_set_response["Status"])
            self.set_state("Changes", change_set_response.get("Changes", []))

            # Get stack ID from change set
            stack_id = change_set_response["StackId"]
            self.set_state("StackId", stack_id)
            self.set_output("StackId", stack_id)

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            error_message = e.response["Error"]["Message"]

            if error_code == "ChangeSetNotFoundException":
                log.error(
                    "Change set '{}' not found for stack '{}'",
                    self.spec.change_set_name,
                    self.spec.stack_name,
                )
                self.set_failed(f"Change set '{self.spec.change_set_name}' not found for stack '{self.spec.stack_name}'")
            else:
                log.error(
                    "Error describing change set '{}': {} - {}",
                    self.spec.change_set_name,
                    error_code,
                    error_message,
                )
                self.set_failed(f"Failed to describe change set '{self.spec.change_set_name}': {error_message}")
            return

        except Exception as e:
            log.error(
                "Unexpected error describing change set '{}': {}",
                self.spec.change_set_name,
                e,
            )
            self.set_failed(f"Unexpected error describing change set '{self.spec.change_set_name}': {e}")
            return

        # Apply the change set
        self.set_running(f"Executing change set '{self.spec.change_set_name}' on stack '{self.spec.stack_name}'")

        try:
            log.info(
                "Executing change set {} on stack {}",
                self.spec.change_set_name,
                self.spec.stack_name,
            )

            cfn_client.execute_change_set(
                StackName=self.spec.stack_name,
                ChangeSetName=self.spec.change_set_name,
            )

            # Update state with execution info
            self.set_state("ChangeSetExecuted", True)
            self.set_state("ExecutionTime", util.get_current_timestamp())
            self.set_state("StackStatus", "UPDATE_IN_PROGRESS")

            log.info("Change set execution initiated for stack {}", self.spec.stack_name)

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            error_message = e.response["Error"]["Message"]

            log.error(
                "Error executing change set '{}': {} - {}",
                self.spec.change_set_name,
                error_code,
                error_message,
            )
            self.set_state("ApplicationResult", "FAILED")
            self.set_state("FailureReason", f"{error_code}: {error_message}")
            self.set_failed(f"Failed to execute change set '{self.spec.change_set_name}': {error_message}")

        except Exception as e:
            log.error(
                "Unexpected error executing change set '{}': {}",
                self.spec.change_set_name,
                e,
            )
            self.set_state("ApplicationResult", "FAILED")
            self.set_state("FailureReason", str(e))
            self.set_failed(f"Unexpected error executing change set '{self.spec.change_set_name}': {e}")

        log.trace("ApplyChangeSetAction execution completed")

    def _check(self):
        """Monitor stack status after executing the change set and finalize results."""
        log.trace("Checking ApplyChangeSetAction")

        stack_id = self.get_state("StackId")

        if not stack_id:
            log.error("Stack ID not found in state - execute may not have run")
            self.set_failed("Stack ID not found in state - execute may not have run")
            return

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

        try:
            # Check stack status
            response = cfn_client.describe_stacks(StackName=stack_id)

            if not response.get("Stacks"):
                log.error("Stack '{}' not found", self.spec.stack_name)
                self.set_failed(f"Stack '{self.spec.stack_name}' not found")
                return

            stack_info = response["Stacks"][0]
            stack_status = stack_info["StackStatus"]
            stack_arn = stack_info.get("StackId", stack_id)

            log.info("Stack status: {}", stack_status)

            # Update state with current status
            self.set_state("StackStatus", stack_status)
            self.set_output("StackStatus", stack_status)
            self.set_output("StackArn", stack_arn)

            if stack_status in ["UPDATE_COMPLETE", "CREATE_COMPLETE"]:
                # Change set applied successfully
                stack_outputs = stack_info.get("Outputs", [])

                # Get detailed resource information
                resources_created, resources_updated, resources_deleted = self._get_stack_resources(cfn_client, stack_id)

                # Set comprehensive state outputs
                self.set_state("ApplicationCompleted", True)
                self.set_state("CompletionTime", util.get_current_timestamp())
                self.set_state("ApplicationResult", "SUCCESS")
                self.set_state("StackOutputs", stack_outputs)
                self.set_state("ResourcesCreated", resources_created)
                self.set_state("ResourcesUpdated", resources_updated)
                self.set_state("ResourcesDeleted", resources_deleted)

                # Set outputs for other actions
                self.set_output("ApplicationCompleted", True)
                self.set_output("ApplicationResult", "SUCCESS")
                self.set_output("StackOutputs", stack_outputs)
                self.set_output("ResourcesCreated", resources_created)
                self.set_output("ResourcesUpdated", resources_updated)
                self.set_output("ResourcesDeleted", resources_deleted)

                total_resources = len(resources_created) + len(resources_updated) + len(resources_deleted)
                self.set_complete(
                    f"Change set {self.spec.change_set_name} applied successfully. {total_resources} resources affected."
                )
                log.info(
                    "Change set {} applied successfully to stack {}",
                    self.spec.change_set_name,
                    self.spec.stack_name,
                )

            elif stack_status in [
                "UPDATE_IN_PROGRESS",
                "CREATE_IN_PROGRESS",
                "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS",
            ]:
                # Still applying
                self.set_running(f"Change set {self.spec.change_set_name} application in progress")

            elif stack_status in [
                "UPDATE_FAILED",
                "CREATE_FAILED",
                "UPDATE_ROLLBACK_COMPLETE",
                "UPDATE_ROLLBACK_IN_PROGRESS",
                "ROLLBACK_COMPLETE",
                "ROLLBACK_IN_PROGRESS",
            ]:
                # Application failed
                status_reason = stack_info.get("StackStatusReason", "Unknown failure")
                log.error("Change set application failed: {}", status_reason)

                self.set_state("ApplicationResult", "FAILED")
                self.set_state("FailureReason", status_reason)
                self.set_failed(f"Change set application failed: {status_reason}")

            else:
                # Unknown status
                log.warning("Unknown stack status: {}", stack_status)
                self.set_running(f"Stack {self.spec.stack_name} in status: {stack_status}")

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            error_message = e.response["Error"]["Message"]

            if error_code == "StackNotFoundException":
                log.error("Stack {} not found", self.spec.stack_name)
                self.set_failed(f"Stack {self.spec.stack_name} not found")
            else:
                log.error("Error checking stack status: {} - {}", error_code, error_message)
                self.set_failed(f"Error checking stack status: {error_message}")

        except Exception as e:
            log.error("Unexpected error checking stack status: {}", e)
            self.set_failed(f"Unexpected error checking stack status: {e}")

        log.trace("ApplyChangeSetAction check completed")

    def _unexecute(self):
        """Best-effort rollback: cancel update or mark manual intervention required."""
        log.trace("Unexecuting ApplyChangeSetAction")

        stack_id = self.get_state("StackId")

        if not stack_id:
            log.info("No stack to rollback - nothing was applied")
            self.set_state("RollbackResult", "NOT_FOUND")
            self.set_complete("No stack to rollback - nothing was applied")
            return

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

        try:
            # Check current stack status
            response = cfn_client.describe_stacks(StackName=stack_id)

            if not response.get("Stacks"):
                log.info("Stack '{}' no longer exists", self.spec.stack_name)
                self.set_state("RollbackResult", "STACK_NOT_FOUND")
                self.set_complete(f"Stack '{self.spec.stack_name}' no longer exists")
                return

            stack_info = response["Stacks"][0]
            stack_status = stack_info["StackStatus"]

            log.info("Current stack status for rollback: {}", stack_status)

            if stack_status in ["UPDATE_IN_PROGRESS", "CREATE_IN_PROGRESS"]:
                # Cancel the in-progress update
                log.info("Canceling in-progress stack update: {}", self.spec.stack_name)

                cfn_client.cancel_update_stack(StackName=stack_id)

                self.set_state("RollbackInitiated", True)
                self.set_state("RollbackTime", util.get_current_timestamp())
                self.set_state("RollbackResult", "CANCEL_INITIATED")

                self.set_complete(f"Stack update cancellation initiated for {self.spec.stack_name}")
                log.info("Stack update cancellation initiated for {}", self.spec.stack_name)

            elif stack_status in ["UPDATE_COMPLETE", "CREATE_COMPLETE"]:
                # Stack update completed, attempt rollback (manual)
                log.warning("Stack rollback for completed updates requires manual intervention or reverse change set")

                self.set_state("RollbackResult", "MANUAL_INTERVENTION_REQUIRED")
                self.set_complete(f"Stack {self.spec.stack_name} rollback requires manual intervention")

            elif stack_status in ["UPDATE_ROLLBACK_COMPLETE", "ROLLBACK_COMPLETE"]:
                # Already rolled back
                log.info(
                    "Stack {} is already in rollback complete state",
                    self.spec.stack_name,
                )
                self.set_state("RollbackResult", "ALREADY_ROLLED_BACK")
                self.set_complete(f"Stack {self.spec.stack_name} is already rolled back")

            else:
                log.warning(
                    "Stack {} is in status {} - rollback may not be applicable",
                    self.spec.stack_name,
                    stack_status,
                )
                self.set_state("RollbackResult", f"NOT_APPLICABLE_{stack_status}")
                self.set_complete(f"Stack {self.spec.stack_name} rollback not applicable for status: {stack_status}")

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            error_message = e.response["Error"]["Message"]

            if error_code == "StackNotFoundException":
                log.info("Stack {} not found during rollback", self.spec.stack_name)
                self.set_state("RollbackResult", "STACK_NOT_FOUND")
                self.set_complete(f"Stack {self.spec.stack_name} not found during rollback")
            else:
                log.error("Error during stack rollback: {} - {}", error_code, error_message)
                self.set_state("RollbackResult", "FAILED")
                self.set_state("FailureReason", f"{error_code}: {error_message}")
                self.set_failed(f"Error during stack rollback: {error_message}")

        except Exception as e:
            log.error("Unexpected error during stack rollback: {}", e)
            self.set_state("RollbackResult", "FAILED")
            self.set_state("FailureReason", str(e))
            self.set_failed(f"Unexpected error during stack rollback: {e}")

        log.trace("ApplyChangeSetAction unexecution completed")

    def _cancel(self):
        """Cancel an in-progress change set application if possible."""
        log.trace("Cancelling ApplyChangeSetAction")

        stack_status = self.get_state("StackStatus")

        if stack_status in ["UPDATE_IN_PROGRESS", "CREATE_IN_PROGRESS"]:
            # Try to cancel the in-progress update
            log.info("Cancelling in-progress change set application")
            self._unexecute()
        else:
            log.info("Change set application not in cancellable state: {}", stack_status)
            self.set_complete(f"Change set application not in cancellable state: {stack_status}")

        log.trace("ApplyChangeSetAction cancellation completed")

    def _get_stack_resources(self, cfn_client, stack_id):
        """Return resources created, updated, or deleted during the operation.

        Args:
          cfn_client: CloudFormation client.
          stack_id: ID or ARN of the stack.

        Returns:
          Tuple (resources_created, resources_updated, resources_deleted),
          where each item is a list of resource info dicts.
        """
        resources_created = []
        resources_updated = []
        resources_deleted = []

        try:
            # Get stack resources
            paginator = cfn_client.get_paginator("describe_stack_resources")
            page_iterator = paginator.paginate(StackName=stack_id)

            for page in page_iterator:
                for resource in page.get("StackResources", []):
                    resource_info = {
                        "LogicalResourceId": resource.get("LogicalResourceId"),
                        "PhysicalResourceId": resource.get("PhysicalResourceId"),
                        "ResourceType": resource.get("ResourceType"),
                        "ResourceStatus": resource.get("ResourceStatus"),
                        "Timestamp": (resource.get("Timestamp").isoformat() if resource.get("Timestamp") else None),
                    }

                    # Categorize based on resource status
                    status = resource.get("ResourceStatus", "")
                    if "CREATE_COMPLETE" in status:
                        resources_created.append(resource_info)
                    elif "UPDATE_COMPLETE" in status:
                        resources_updated.append(resource_info)
                    elif "DELETE_COMPLETE" in status:
                        resources_deleted.append(resource_info)

        except Exception as e:
            log.warning("Failed to get detailed stack resources: {}", e)
            # Return empty lists if we can't get resource details

        return resources_created, resources_updated, resources_deleted

    @classmethod
    def generate_action_resource(cls, **kwargs) -> ApplyChangeSetActionResource:
        """Factory: create a typed ApplyChangeSetActionResource."""
        return ApplyChangeSetActionResource(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> ApplyChangeSetActionSpec:
        """Factory: create typed ApplyChangeSetActionSpec."""
        return ApplyChangeSetActionSpec(**kwargs)
