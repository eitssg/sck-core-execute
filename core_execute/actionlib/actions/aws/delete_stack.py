"""Delete a CloudFormation stack and track progress.

Starts deletion, monitors status, records failures, and exposes results in state/outputs.
"""

from typing import Any
from pydantic import Field, model_validator
from botocore.exceptions import ClientError

import core_logging as log

from core_framework.models import DeploymentDetails, ActionResource, ActionSpec

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


class DeleteStackActionSpec(ActionSpec):
    """Parameters for deleting a CloudFormation stack.

    Attributes:
      account: AWS account ID for the stack.
      region: AWS region where the stack resides.
      stack_name: Name or ARN of the stack to delete.
      success_statuses: Stack statuses that should be treated as success (skip delete).
    """

    stack_name: str = Field(
        ...,
        alias="StackName",
        description="The name of the stack to delete (required)",
    )
    success_statuses: list[str] = Field(
        default_factory=list,
        alias="SuccessStatuses",
        description="The stack statuses that indicate success (optional). Defaults to []",
    )


class DeleteStackActionResource(ActionResource):
    """Resource model for DeleteStack (normalizes kind/spec)."""

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Normalize incoming values and enforce canonical kind/spec."""
        if not isinstance(values, dict):
            return values

        if "Kind" in values:
            del values["Kind"]

        values["kind"] = "AWS::DeleteStack"

        return values


class DeleteStackAction(BaseAction[DeleteStackActionSpec]):
    """Delete a CloudFormation stack and monitor until completion.

    Handles common edge cases (already deleted, in progress, failed) and
    records failed resources and recent events for troubleshooting.
    """

    def __init__(
        self,
        definition: ActionResource,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        """Initialize the action and validate parameters."""
        super().__init__(definition, context, deployment_details)

        # Validate and set the parameters
        self.spec = DeleteStackActionSpec.model_validate(definition.spec)

    def _resolve(self):
        """Render template variables in account, region, and stack_name."""
        log.trace("Resolving DeleteStackAction")

        self.spec.account = self.renderer.render_string(self.spec.account, self.context)
        self.spec.region = self.renderer.render_string(self.spec.region, self.context)
        self.spec.stack_name = self.renderer.render_string(self.spec.stack_name, self.context)

        log.trace("DeleteStackAction resolved")

    def can_initialize(self) -> bool:
        """Return True if the action can be reinitialized (no deletion in progress)."""
        stack_status = self.get_state("CurrentStackStatus") or self.get_state("InitialStackStatus")
        if stack_status and "DELETE_IN_PROGRESS" in stack_status:
            log.warning(
                "Cannot reinitialize DeleteStackAction - stack deletion in progress: {}",
                stack_status,
            )
            return False

        return True

    def initialize(self) -> bool:
        """Clear deletion-specific state so teardown can rerun safely.


        Returns:
            bool: True if initialization was successful, False otherwise.

        """
        log.info("Initializing DeleteStackAction {} for teardown rerun", self.name)

        # Call super to clear basic state
        super().initialize()

        # Clear delete-specific state but keep stack identification
        delete_state_keys = [
            "DeletionStarted",
            "DeletionInitiated",
            "DeletionCompleted",
            "DeletionResult",
            "StackExists",
            "CurrentStackStatus",
            "CompletionTime",
            "DeletionInitiatedTime",
            "StartTime",
            "DeletionError",
            "FailedResources",
            "RecentStackEvents",
            "RollbackAttempted",
            "RollbackResult",
            "UnexpectedStatus",
        ]

        for key in delete_state_keys:
            state_key = f"{self.name}/{key}"
            if state_key in self.context:
                self.context.pop(state_key)
                log.trace("Cleared deletion state: {}", state_key)

        log.info("DeleteStackAction {} reinitialized for teardown", self.name)

        return True

    def can_execute(self) -> bool:
        """Validate parameters and test AWS connectivity before execution."""
        # Basic parameter validation
        if not self.spec.stack_name:
            log.error("Cannot execute teardown - StackName is required")
            return False

        # Test AWS connectivity before starting teardown
        try:
            cfn_client = aws.cfn_client(
                region=self.spec.region,
                role_arn=util.get_provisioning_role_arn(self.spec.account),
            )

            # Quick connectivity test
            cfn_client.list_stacks(MaxItems=1)
            return True

        except Exception as e:
            log.error("Cannot execute teardown - AWS connectivity failed: {}", e)
            return False

    def _execute(self):
        """Initiate deletion and set initial state; handle common statuses."""
        log.trace("Executing DeleteStackAction")

        # Validate required parameters
        if not self.spec.stack_name or self.spec.stack_name == "":
            self.set_failed("StackName parameter is required")
            log.error("StackName parameter is required")
            return

        # Set initial state information
        self.set_state("StackName", self.spec.stack_name)
        self.set_state("Region", self.spec.region)
        self.set_state("Account", self.spec.account)
        self.set_state("DeletionStarted", True)
        self.set_state("StartTime", util.get_current_timestamp())

        # Set outputs for other actions to reference
        self.set_output("StackName", self.spec.stack_name)
        self.set_output("Region", self.spec.region)
        self.set_output("DeletionStarted", True)

        # Obtain a CloudFormation client
        try:
            cfn_client = aws.cfn_client(
                region=self.spec.region,
                role_arn=util.get_provisioning_role_arn(self.spec.account),
            )
        except Exception as e:
            log.error("Failed to create CloudFormation client: {}", e)
            self.set_failed(f"Failed to create CloudFormation client: {e}")
            return

        # Check current stack status
        stack_info = self._get_stack_status(cfn_client)

        if not stack_info["exists"]:
            # Stack doesn't exist - already deleted (common in teardown reruns)
            self.set_state("StackExists", False)
            self.set_state("DeletionCompleted", True)
            self.set_state("CompletionTime", util.get_current_timestamp())
            self.set_state("DeletionResult", "ALREADY_DELETED")

            self.set_output("StackStatus", "DELETE_COMPLETE")
            self.set_output("DeletionCompleted", True)
            self.set_output("DeletionResult", "ALREADY_DELETED")

            log.info(
                "Stack '{}' does not exist - already deleted (common in teardown)",
                self.spec.stack_name,
            )
            self.set_complete(f"Stack '{self.spec.stack_name}' already deleted")
            return

        # Stack exists - store initial information
        stack_status = stack_info["status"]
        stack_id = stack_info["stack_id"]

        self.set_state("StackExists", True)
        self.set_state("StackId", stack_id)
        self.set_state("InitialStackStatus", stack_status)
        self.set_output("StackId", stack_id)
        self.set_output("StackStatus", stack_status)

        log.info(
            "Found stack '{}' with status '{}' for teardown",
            self.spec.stack_name,
            stack_status,
        )

        if stack_status == "DELETE_COMPLETE":
            # Stack already deleted
            self.set_state("DeletionCompleted", True)
            self.set_state("CompletionTime", util.get_current_timestamp())
            self.set_state("DeletionResult", "ALREADY_DELETED")

            self.set_output("DeletionCompleted", True)
            self.set_output("DeletionResult", "ALREADY_DELETED")

            self.set_complete(f"Stack '{self.spec.stack_name}' has been previously deleted")
            return

        elif "DELETE_IN_PROGRESS" in stack_status:
            # Stack deletion already in progress (parallel execution or rerun)
            log.info(
                "Stack '{}' deletion already in progress with status '{}' (parallel execution)",
                self.spec.stack_name,
                stack_status,
            )
            self.set_running(f"Stack '{self.spec.stack_name}' deletion already in progress")
            return

        elif stack_status in [
            "CREATE_FAILED",
            "ROLLBACK_COMPLETE",
            "UPDATE_ROLLBACK_COMPLETE",
        ]:
            # Stack is in a failed state - can still be deleted
            log.info(
                "Stack '{}' is in failed state '{}' - proceeding with deletion",
                self.spec.stack_name,
                stack_status,
            )
            # Continue to deletion logic below

        elif stack_status in self.spec.success_statuses:
            # Stack is in a success status - don't delete if configured
            log.warning(
                "Stack '{}' has status '{}' which is configured as success - skipping deletion",
                self.spec.stack_name,
                stack_status,
            )
            self.set_state("DeletionCompleted", True)
            self.set_state("CompletionTime", util.get_current_timestamp())
            self.set_state("DeletionResult", "SKIPPED_SUCCESS_STATUS")

            self.set_output("DeletionCompleted", True)
            self.set_output("DeletionResult", "SKIPPED_SUCCESS_STATUS")

            self.set_complete(f"Stack '{self.spec.stack_name}' not deleted - status '{stack_status}' is configured as success")
            return

        # Initiate stack deletion
        try:
            log.info("Initiating teardown deletion of stack '{}'", self.spec.stack_name)
            cfn_client.delete_stack(StackName=stack_id)

            self.set_state("DeletionInitiated", True)
            self.set_state("DeletionInitiatedTime", util.get_current_timestamp())

            self.set_running(f"Teardown: Deleting stack '{self.spec.stack_name}'")

        except ClientError as e:
            error_code, error_message = self.parse_client_error(e)

            # Handle common teardown errors gracefully
            if error_code == "ValidationError" and "does not exist" in error_message:
                # Stack was deleted between status check and delete call
                log.info(
                    "Stack '{}' no longer exists (deleted during execution)",
                    self.spec.stack_name,
                )
                self.set_state("DeletionCompleted", True)
                self.set_state("DeletionResult", "ALREADY_DELETED")
                self.set_complete(f"Stack '{self.spec.stack_name}' already deleted")
                return

            log.error(
                "Failed to initiate stack deletion for '{}': {} - {}",
                self.spec.stack_name,
                error_code,
                error_message,
            )

            self.set_state("DeletionInitiated", False)
            self.set_state("DeletionError", f"{error_code}: {error_message}")

            self.set_failed(f"Failed to initiate stack deletion: {error_message}")
            return

        except Exception as e:
            log.error("Unexpected error initiating stack deletion: {}", e)
            self.set_failed(f"Unexpected error initiating stack deletion: {e}")
            return

        log.trace("DeleteStackAction execution completed")

    def _check(self):
        """Monitor deletion progress and update state/outputs accordingly."""
        log.trace("Checking DeleteStackAction")

        # Obtain a CloudFormation client
        try:
            cfn_client = aws.cfn_client(
                region=self.spec.region,
                role_arn=util.get_provisioning_role_arn(self.spec.account),
            )
        except Exception as e:
            log.error("Failed to create CloudFormation client: {}", e)
            self.set_failed(f"Failed to create CloudFormation client: {e}")
            return

        # Get current stack status
        stack_info = self._get_stack_status(cfn_client)

        if not stack_info["exists"]:
            # Stack no longer exists - deletion completed successfully
            self.set_state("StackExists", False)
            self.set_state("DeletionCompleted", True)
            self.set_state("CompletionTime", util.get_current_timestamp())
            self.set_state("DeletionResult", "SUCCESS")

            self.set_output("StackStatus", "DELETE_COMPLETE")
            self.set_output("DeletionCompleted", True)
            self.set_output("DeletionResult", "SUCCESS")

            self.set_complete(f"Stack '{self.spec.stack_name}' successfully deleted")
            return

        # Stack still exists - check status
        stack_status = stack_info["status"]
        self.set_state("CurrentStackStatus", stack_status)
        self.set_output("StackStatus", stack_status)

        if stack_status == "DELETE_COMPLETE":
            # Stack marked as deleted
            self.set_state("DeletionCompleted", True)
            self.set_state("CompletionTime", util.get_current_timestamp())
            self.set_state("DeletionResult", "SUCCESS")

            self.set_output("DeletionCompleted", True)
            self.set_output("DeletionResult", "SUCCESS")

            self.set_complete(f"Stack '{self.spec.stack_name}' deletion completed")
            return

        elif "DELETE_IN_PROGRESS" in stack_status:
            # Deletion still in progress
            log.debug(
                "Stack '{}' deletion in progress with status '{}'",
                self.spec.stack_name,
                stack_status,
            )

            # Track failed resources if available
            self._track_stack_events(cfn_client)

            self.set_running(f"Stack '{self.spec.stack_name}' deletion in progress (status: {stack_status})")
            return

        elif stack_status == "DELETE_FAILED":
            # Deletion failed - track failed resources
            log.error("Stack '{}' deletion failed", self.spec.stack_name)

            self._track_stack_events(cfn_client)
            failed_resources = self._get_failed_resources(cfn_client)

            self.set_state("DeletionCompleted", True)
            self.set_state("CompletionTime", util.get_current_timestamp())
            self.set_state("DeletionResult", "FAILED")
            self.set_state("FailedResources", failed_resources)

            self.set_output("DeletionCompleted", True)
            self.set_output("DeletionResult", "FAILED")
            self.set_output("FailedResources", failed_resources)

            self.set_failed(
                f"Stack '{self.spec.stack_name}' deletion failed. {len(failed_resources)} resources could not be deleted."
            )
            return

        elif stack_status in self.spec.success_statuses:
            # Stack is in a configured success status
            self.set_state("DeletionCompleted", True)
            self.set_state("CompletionTime", util.get_current_timestamp())
            self.set_state("DeletionResult", "SKIPPED_SUCCESS_STATUS")

            self.set_output("DeletionCompleted", True)
            self.set_output("DeletionResult", "SKIPPED_SUCCESS_STATUS")

            self.set_complete(f"Stack '{self.spec.stack_name}' not deleted - status '{stack_status}' is configured as success")
            return

        else:
            # Unexpected status
            log.warning(
                "Stack '{}' has unexpected status '{}'",
                self.spec.stack_name,
                stack_status,
            )

            self.set_state("DeletionCompleted", True)
            self.set_state("CompletionTime", util.get_current_timestamp())
            self.set_state("DeletionResult", "UNEXPECTED_STATUS")
            self.set_state("UnexpectedStatus", stack_status)

            self.set_output("DeletionResult", "UNEXPECTED_STATUS")
            self.set_output("UnexpectedStatus", stack_status)

            self.set_failed(f"Stack '{self.spec.stack_name}' has unexpected status '{stack_status}'")
            return

        log.trace("DeleteStackAction check completed")

    def _unexecute(self):
        """No rollback; stack deletion cannot be undone."""
        log.trace("Unexecuting DeleteStackAction")

        # Stack deletion cannot be undone
        log.warning(
            "Stack deletion cannot be rolled back - Stack '{}' remains in its current state",
            self.spec.stack_name,
        )

        self.set_state("RollbackAttempted", True)
        self.set_state("RollbackResult", "NOT_POSSIBLE")

        self.set_complete("Stack deletion cannot be rolled back")

        log.trace("DeleteStackAction unexecution completed")

    def _cancel(self):
        """No-op; CloudFormation cannot cancel a deletion in progress."""
        log.trace("Cancelling DeleteStackAction")

        # Stack deletion cannot be cancelled once started
        self.set_complete("Stack deletion operations cannot be cancelled")

        log.trace("DeleteStackAction cancellation completed")

    def _get_stack_status(self, cfn_client) -> dict[str, Any]:
        """Return current stack status and metadata.

        Args:
          cfn_client: boto3 CloudFormation client.

        Returns:
          Dict with keys:
            - exists: bool
            - status: str (if exists)
            - stack_id: str (if exists)
            - creation_time: datetime | None
            - last_updated_time: datetime | None
            - stack_name: str (if exists)

        Raises:
          ClientError: For unexpected CloudFormation errors.
          Exception: For other unexpected errors.
        """
        try:
            response = cfn_client.describe_stacks(StackName=self.spec.stack_name)
            stack = response["Stacks"][0]

            return {
                "exists": True,
                "status": stack["StackStatus"],
                "stack_id": stack["StackId"],
                "creation_time": stack.get("CreationTime"),
                "last_updated_time": stack.get("LastUpdatedTime"),
                "stack_name": stack["StackName"],
            }

        except ClientError as e:
            error_code, error_message = self.parse_client_error(e)

            if "does not exist" in error_message or error_code == "ValidationError":
                log.debug("Stack '{}' does not exist", self.spec.stack_name)
                return {"exists": False}
            else:
                log.error(
                    "Error describing stack '{}': {} - {}",
                    self.spec.stack_name,
                    error_code,
                    error_message,
                )
                raise

        except Exception as e:
            log.error("Unexpected error describing stack '{}': {}", self.spec.stack_name, e)
            raise

    def _track_stack_events(self, cfn_client):
        """Collect and store recent stack events for debugging.

        Args:
          cfn_client: boto3 CloudFormation client.
        """
        try:
            stack_id = self.get_state("StackId")
            if not stack_id:
                return

            response = cfn_client.describe_stack_events(StackName=stack_id)
            events = response.get("StackEvents", [])

            # Get recent events (last 10)
            recent_events = []
            for event in events[:10]:
                recent_events.append(
                    {
                        "Timestamp": (event.get("Timestamp").isoformat() if event.get("Timestamp") else None),
                        "LogicalResourceId": event.get("LogicalResourceId"),
                        "ResourceType": event.get("ResourceType"),
                        "ResourceStatus": event.get("ResourceStatus"),
                        "ResourceStatusReason": event.get("ResourceStatusReason"),
                    }
                )

            self.set_state("RecentStackEvents", recent_events)

        except Exception as e:
            log.warning("Failed to retrieve stack events: {}", e)

    def _get_failed_resources(self, cfn_client) -> list[dict[str, Any]]:
        """Return a list of resources that failed to delete.

        Args:
          cfn_client: boto3 CloudFormation client.

        Returns:
          List of dicts with failed resource details.
        """
        failed_resources = []

        try:
            stack_id = self.get_state("StackId")
            if not stack_id:
                return failed_resources

            response = cfn_client.list_stack_resources(StackName=stack_id)
            resources = response.get("StackResourceSummaries", [])

            for resource in resources:
                resource_status = resource.get("ResourceStatus", "")
                if "DELETE_FAILED" in resource_status:
                    failed_resources.append(
                        {
                            "LogicalResourceId": resource.get("LogicalResourceId"),
                            "PhysicalResourceId": resource.get("PhysicalResourceId"),
                            "ResourceType": resource.get("ResourceType"),
                            "ResourceStatus": resource_status,
                            "ResourceStatusReason": resource.get("ResourceStatusReason"),
                            "LastUpdatedTimestamp": (
                                resource.get("LastUpdatedTimestamp").isoformat() if resource.get("LastUpdatedTimestamp") else None
                            ),
                        }
                    )

        except Exception as e:
            log.warning("Failed to retrieve failed resources: {}", e)

        return failed_resources

    @classmethod
    def generate_action_resource(cls, **kwargs) -> DeleteStackActionResource:
        """Factory: create a typed DeleteStackActionResource."""
        return DeleteStackActionResource.model_validate(kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> DeleteStackActionSpec:
        """Factory: create typed DeleteStackActionSpec."""
        return DeleteStackActionSpec.model_validate(kwargs)
