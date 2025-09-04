"""Delete a CloudFormation stack"""

from typing import Any
from pydantic import Field, model_validator
from botocore.exceptions import ClientError

import core_logging as log

from core_framework.models import DeploymentDetails, ActionResource, ActionSpec

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


class DeleteStackActionSpec(ActionSpec):
    """
    Parameters for the DeleteStackAction.

    :param account: The account to use for the action (required)
    :type account: str
    :param region: The region where the stack is located (required)
    :type region: str
    :param stack_name: The name of the stack to delete (required)
    :type stack_name: str
    :param success_statuses: Stack statuses that indicate success (optional)
    :type success_statuses: list[str]
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
    """
    Generate the action definition for DeleteStackAction.

    This class provides default values and validation for DeleteStackAction parameters.

    :param values: Dictionary of action specification values
    :type values: dict[str, Any]
    :return: Validated action specification values
    :rtype: dict[str, Any]
    """

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:

        if not isinstance(values, dict):
            return values

        values.pop("kind", None)
        values.pop("Kind", None)
        values["kind"] = "AWS::DeleteStack"

        spec = values.pop("spec", None) or values.pop("Spec", None)
        if isinstance(spec, dict):
            values["spec"] = spec
        elif isinstance(spec, DeleteStackActionSpec):
            values["spec"] = spec.model_dump()

        return values


class DeleteStackAction(BaseAction):
    """
    Delete a CloudFormation stack.

    This action will delete a CloudFormation stack and monitor the deletion process
    until completion. The action handles various stack states and tracks any resources
    that fail to delete.

    :param definition: The action specification containing configuration details
    :type definition: ActionResource
    :param context: The Jinja2 rendering context containing all variables
    :type context: dict[str, Any]
    :param deployment_details: Client/portfolio/app/branch/build information
    :type deployment_details: DeploymentDetails

    .. rubric:: Parameters

    :Name: Enter a name to define this action instance
    :Kind: Use the value ``AWS::DeleteStack``
    :Spec.Account: The account where the stack is located (required)
    :Spec.Region: The region where the stack is located (required)
    :Spec.StackName: The name of the stack to delete (required)
    :Spec.SuccessStatuses: Stack statuses that indicate success (optional)

    .. rubric:: ActionResource Example

    .. code-block:: yaml

        - Name: action-aws-deletestack-name
          Kind: "AWS::DeleteStack"
          Spec:
            Account: "154798051514"
            Region: "ap-southeast-1"
            StackName: "my-application-stack-name"
            SuccessStatuses: ["UPDATE_COMPLETE", "CREATE_COMPLETE"]
          Scope: "build"

    .. note::
        Stack deletion can take several hours depending on resources involved.

    .. warning::
        Some resources may fail to delete due to dependencies or protection settings.
    """

    def __init__(
        self,
        definition: ActionResource,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        # Validate and set the parameters
        self.params = DeleteStackActionSpec(**definition.spec)

    def _resolve(self):
        """
        Resolve template variables in action parameters.

        This method renders Jinja2 templates in the action parameters using the current context.
        """
        log.trace("Resolving DeleteStackAction")

        self.params.account = self.renderer.render_string(self.params.account, self.context)
        self.params.region = self.renderer.render_string(self.params.region, self.context)
        self.params.stack_name = self.renderer.render_string(self.params.stack_name, self.context)

        log.trace("DeleteStackAction resolved")

    def can_initialize(self) -> bool:
        """
        Check if DeleteStackAction can be reinitialized for teardown reruns.

        Delete actions can generally be reinitialized unless they're in
        the middle of a deletion operation.
        """
        stack_status = self.get_state("CurrentStackStatus") or self.get_state("InitialStackStatus")
        if stack_status and "DELETE_IN_PROGRESS" in stack_status:
            log.warning("Cannot reinitialize DeleteStackAction - stack deletion in progress: {}", stack_status)
            return False

        return True

    def initialize(self):
        """
        Initialize DeleteStackAction for teardown rerun.

        Clears deletion-specific state while preserving configuration.
        Allows the action to rediscover stack state during execution.
        """
        log.info("Initializing DeleteStackAction {} for teardown rerun", self.name)

        # Call parent to clear basic state
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

    def can_execute(self) -> bool:
        """
        Check if DeleteStackAction can execute in teardown pipeline.

        Enhanced validation for CD pipeline robustness.
        """
        # Basic parameter validation
        if not self.params.stack_name:
            log.error("Cannot execute teardown - StackName is required")
            return False

        # Test AWS connectivity before starting teardown
        try:
            cfn_client = aws.cfn_client(
                region=self.params.region,
                role=util.get_provisioning_role_arn(self.params.account),
            )

            # Quick connectivity test
            cfn_client.list_stacks(MaxItems=1)
            return True

        except Exception as e:
            log.error("Cannot execute teardown - AWS connectivity failed: {}", e)
            return False

    def _execute(self):
        """
        Execute the stack deletion operation with enhanced teardown logic.

        This method initiates the deletion of the CloudFormation stack and sets up
        monitoring for the deletion process.

        Enhanced for CD pipeline teardown with better error handling and
        parallel-friendly state management.
        """
        log.trace("Executing DeleteStackAction")

        # Validate required parameters
        if not self.params.stack_name or self.params.stack_name == "":
            self.set_failed("StackName parameter is required")
            log.error("StackName parameter is required")
            return

        # Set initial state information
        self.set_state("StackName", self.params.stack_name)
        self.set_state("Region", self.params.region)
        self.set_state("Account", self.params.account)
        self.set_state("DeletionStarted", True)
        self.set_state("StartTime", util.get_current_timestamp())

        # Set outputs for other actions to reference
        self.set_output("StackName", self.params.stack_name)
        self.set_output("Region", self.params.region)
        self.set_output("DeletionStarted", True)

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

            log.info("Stack '{}' does not exist - already deleted (common in teardown)", self.params.stack_name)
            self.set_complete(f"Stack '{self.params.stack_name}' already deleted")
            return

        # Stack exists - store initial information
        stack_status = stack_info["status"]
        stack_id = stack_info["stack_id"]

        self.set_state("StackExists", True)
        self.set_state("StackId", stack_id)
        self.set_state("InitialStackStatus", stack_status)
        self.set_output("StackId", stack_id)
        self.set_output("StackStatus", stack_status)

        log.info("Found stack '{}' with status '{}' for teardown", self.params.stack_name, stack_status)

        if stack_status == "DELETE_COMPLETE":
            # Stack already deleted
            self.set_state("DeletionCompleted", True)
            self.set_state("CompletionTime", util.get_current_timestamp())
            self.set_state("DeletionResult", "ALREADY_DELETED")

            self.set_output("DeletionCompleted", True)
            self.set_output("DeletionResult", "ALREADY_DELETED")

            self.set_complete(f"Stack '{self.params.stack_name}' has been previously deleted")
            return

        elif "DELETE_IN_PROGRESS" in stack_status:
            # Stack deletion already in progress (parallel execution or rerun)
            log.info(
                "Stack '{}' deletion already in progress with status '{}' (parallel execution)",
                self.params.stack_name,
                stack_status,
            )
            self.set_running(f"Stack '{self.params.stack_name}' deletion already in progress")
            return

        elif stack_status in ["CREATE_FAILED", "ROLLBACK_COMPLETE", "UPDATE_ROLLBACK_COMPLETE"]:
            # Stack is in a failed state - can still be deleted
            log.info(
                "Stack '{}' is in failed state '{}' - proceeding with deletion",
                self.params.stack_name,
                stack_status,
            )
            # Continue to deletion logic below

        elif stack_status in self.params.success_statuses:
            # Stack is in a success status - don't delete if configured
            log.warning(
                "Stack '{}' has status '{}' which is configured as success - skipping deletion",
                self.params.stack_name,
                stack_status,
            )
            self.set_state("DeletionCompleted", True)
            self.set_state("CompletionTime", util.get_current_timestamp())
            self.set_state("DeletionResult", "SKIPPED_SUCCESS_STATUS")

            self.set_output("DeletionCompleted", True)
            self.set_output("DeletionResult", "SKIPPED_SUCCESS_STATUS")

            self.set_complete(f"Stack '{self.params.stack_name}' not deleted - status '{stack_status}' is configured as success")
            return

        # Initiate stack deletion
        try:
            log.info("Initiating teardown deletion of stack '{}'", self.params.stack_name)
            cfn_client.delete_stack(StackName=stack_id)

            self.set_state("DeletionInitiated", True)
            self.set_state("DeletionInitiatedTime", util.get_current_timestamp())

            self.set_running(f"Teardown: Deleting stack '{self.params.stack_name}'")

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            error_message = e.response["Error"]["Message"]

            # Handle common teardown errors gracefully
            if error_code == "ValidationError" and "does not exist" in error_message:
                # Stack was deleted between status check and delete call
                log.info("Stack '{}' no longer exists (deleted during execution)", self.params.stack_name)
                self.set_state("DeletionCompleted", True)
                self.set_state("DeletionResult", "ALREADY_DELETED")
                self.set_complete(f"Stack '{self.params.stack_name}' already deleted")
                return

            log.error(
                "Failed to initiate stack deletion for '{}': {} - {}",
                self.params.stack_name,
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
        """
        Check the status of the stack deletion operation.

        This method monitors the progress of the stack deletion and handles
        various completion and error scenarios.
        """
        log.trace("Checking DeleteStackAction")

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

            self.set_complete(f"Stack '{self.params.stack_name}' successfully deleted")
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

            self.set_complete(f"Stack '{self.params.stack_name}' deletion completed")
            return

        elif "DELETE_IN_PROGRESS" in stack_status:
            # Deletion still in progress
            log.debug(
                "Stack '{}' deletion in progress with status '{}'",
                self.params.stack_name,
                stack_status,
            )

            # Track failed resources if available
            self._track_stack_events(cfn_client)

            self.set_running(f"Stack '{self.params.stack_name}' deletion in progress (status: {stack_status})")
            return

        elif stack_status == "DELETE_FAILED":
            # Deletion failed - track failed resources
            log.error("Stack '{}' deletion failed", self.params.stack_name)

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
                f"Stack '{self.params.stack_name}' deletion failed. {len(failed_resources)} resources could not be deleted."
            )
            return

        elif stack_status in self.params.success_statuses:
            # Stack is in a configured success status
            self.set_state("DeletionCompleted", True)
            self.set_state("CompletionTime", util.get_current_timestamp())
            self.set_state("DeletionResult", "SKIPPED_SUCCESS_STATUS")

            self.set_output("DeletionCompleted", True)
            self.set_output("DeletionResult", "SKIPPED_SUCCESS_STATUS")

            self.set_complete(f"Stack '{self.params.stack_name}' not deleted - status '{stack_status}' is configured as success")
            return

        else:
            # Unexpected status
            log.warning(
                "Stack '{}' has unexpected status '{}'",
                self.params.stack_name,
                stack_status,
            )

            self.set_state("DeletionCompleted", True)
            self.set_state("CompletionTime", util.get_current_timestamp())
            self.set_state("DeletionResult", "UNEXPECTED_STATUS")
            self.set_state("UnexpectedStatus", stack_status)

            self.set_output("DeletionResult", "UNEXPECTED_STATUS")
            self.set_output("UnexpectedStatus", stack_status)

            self.set_failed(f"Stack '{self.params.stack_name}' has unexpected status '{stack_status}'")
            return

        log.trace("DeleteStackAction check completed")

    def _unexecute(self):
        """
        Rollback the stack deletion operation.

        .. note::
            Stack deletion cannot be undone. This method is a no-op.
        """
        log.trace("Unexecuting DeleteStackAction")

        # Stack deletion cannot be undone
        log.warning(
            "Stack deletion cannot be rolled back - Stack '{}' remains in its current state",
            self.params.stack_name,
        )

        self.set_state("RollbackAttempted", True)
        self.set_state("RollbackResult", "NOT_POSSIBLE")

        self.set_complete("Stack deletion cannot be rolled back")

        log.trace("DeleteStackAction unexecution completed")

    def _cancel(self):
        """
        Cancel the stack deletion operation.

        .. note::
            Stack deletion operations in progress cannot be cancelled through CloudFormation.
        """
        log.trace("Cancelling DeleteStackAction")

        # Stack deletion cannot be cancelled once started
        self.set_complete("Stack deletion operations cannot be cancelled")

        log.trace("DeleteStackAction cancellation completed")

    def _get_stack_status(self, cfn_client) -> dict[str, Any]:
        """
        Get the current status of the CloudFormation stack.

        :param cfn_client: CloudFormation client
        :type cfn_client: boto3.client
        :return: Dictionary with stack information
        :rtype: dict[str, Any]
        """
        try:
            response = cfn_client.describe_stacks(StackName=self.params.stack_name)
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
            error_code = e.response["Error"]["Code"]
            error_message = e.response["Error"]["Message"]

            if "does not exist" in error_message or error_code == "ValidationError":
                log.debug("Stack '{}' does not exist", self.params.stack_name)
                return {"exists": False}
            else:
                log.error(
                    "Error describing stack '{}': {} - {}",
                    self.params.stack_name,
                    error_code,
                    error_message,
                )
                raise

        except Exception as e:
            log.error("Unexpected error describing stack '{}': {}", self.params.stack_name, e)
            raise

    def _track_stack_events(self, cfn_client):
        """
        Track and store recent stack events for debugging purposes.

        :param cfn_client: CloudFormation client
        :type cfn_client: boto3.client
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
        """
        Get list of resources that failed to delete.

        :param cfn_client: CloudFormation client
        :type cfn_client: boto3.client
        :return: List of failed resources
        :rtype: list[dict[str, Any]]
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
        return DeleteStackActionResource(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> DeleteStackActionSpec:
        return DeleteStackActionSpec(**kwargs)
