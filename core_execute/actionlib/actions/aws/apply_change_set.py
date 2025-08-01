from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator
from botocore.exceptions import ClientError

import core_logging as log
import core_framework as util

from core_framework.models import ActionSpec, ActionParams, DeploymentDetails
from core_execute.actionlib.action import BaseAction
import core_helper.aws as aws


class ApplyChangeSetActionParams(ActionParams):
    """Parameters for the ApplyChangeSetAction

    This class defines the parameters that can be used in the action.
    """

    stack_name: str = Field(
        ...,
        alias="StackName",
        description="The name of the stack to apply the change set",
    )
    change_set_name: str = Field(..., alias="ChangeSetName", description="The name of the change set to apply")


class ApplyChangeSetActionSpec(ActionSpec):
    """Generate the action definition"""

    @model_validator(mode="before")
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Validate the parameters for the ApplyChangeSetActionSpec"""
        if not (values.get("name") or values.get("Name")):
            values["name"] = "action-aws-applychangeset-name"
        if not (values.get("kind") or values.get("Kind")):
            values["kind"] = "AWS::ApplyChangeSet"
        if not (values.get("depends_on") or values.get("DependsOn")):
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


class ApplyChangeSetAction(BaseAction):
    """CloudFormation Change Set Application Action

    This action applies a CloudFormation change set to execute the changes on a stack.
    It supports cross-account deployments via role assumption and handles
    step function execution patterns with state persistence.

    Kind: Use the value: ``AWS::ApplyChangeSet``

    .. rubric: ActionSpec:

    .. tip:: s3:/<bucket>/artifacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Name: action-aws-applychangeset-name
              Kind: "AWS::ApplyChangeSet"
              Params:
                Account: "154798051514"
                Region: "ap-southeast-1"
                StackName: "my-stack"
                ChangeSetName: "my-changeset"
              Scope: "portfolio"

    State Variables:
        - ChangeSetApplicationStarted: Timestamp when application began
        - ChangeSetName: Name of the applied change set
        - StackName: Name of the target stack
        - StackStatus: Current status of the stack
        - StackId: ID of the target stack
        - ApplicationResult: Result of the change set application

    Output Variables:
        - StackArn: ARN of the updated stack
        - StackId: ID of the updated stack
        - StackStatus: Final status of the stack
        - ResourcesCreated: List of resources that were created
        - ResourcesUpdated: List of resources that were updated
        - ResourcesDeleted: List of resources that were deleted
        - StackOutputs: Stack outputs after application
    """

    def __init__(
        self,
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        self.params = ApplyChangeSetActionParams(**definition.params)

    def _resolve(self):
        """
        Resolve template variables in action parameters.

        This method renders Jinja2 templates in the action parameters using the current context.
        """
        log.trace("Resolving ApplyChangeSetAction")

        self.params.account = self.renderer.render_string(self.params.account, self.context)
        self.params.region = self.renderer.render_string(self.params.region, self.context)
        self.params.stack_name = self.renderer.render_string(self.params.stack_name, self.context)
        self.params.change_set_name = self.renderer.render_string(self.params.change_set_name, self.context)

        log.trace("ApplyChangeSetAction resolved")

    def _execute(self):
        """
        Execute the CloudFormation change set application operation.

        This method applies the specified CloudFormation change set and sets appropriate
        state outputs for tracking.

        :raises: Sets action to failed if parameters are missing or CloudFormation operation fails
        """
        log.trace("Executing ApplyChangeSetAction")

        # Validate required parameters
        if not self.params.stack_name or self.params.stack_name == "":
            self.set_failed("StackName parameter is required")
            log.error("StackName parameter is required")
            return

        if not self.params.change_set_name or self.params.change_set_name == "":
            self.set_failed("ChangeSetName parameter is required")
            log.error("ChangeSetName parameter is required")
            return

        # Check if change set application already started (idempotent execution)
        if self.get_state("ChangeSetApplicationStarted") and self.get_state("StackId"):
            log.info(
                "Change set application already in progress for {}",
                self.params.change_set_name,
            )
            self.set_running(f"Change set application already in progress for {self.params.change_set_name}")
            return

        # Set initial state information
        self.set_state("ChangeSetName", self.params.change_set_name)
        self.set_state("StackName", self.params.stack_name)
        self.set_state("Region", self.params.region)
        self.set_state("Account", self.params.account)
        self.set_state("ChangeSetApplicationStarted", True)
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

        # Verify change set exists and get its details
        try:
            change_set_response = cfn_client.describe_change_set(
                StackName=self.params.stack_name,
                ChangeSetName=self.params.change_set_name,
            )

            if change_set_response["Status"] != "CREATE_COMPLETE":
                log.error(
                    "Change set '{}' is not in CREATE_COMPLETE status: {}",
                    self.params.change_set_name,
                    change_set_response["Status"],
                )
                self.set_failed(
                    f"Change set '{self.params.change_set_name}' is not ready for execution: {change_set_response['Status']}"
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
                    self.params.change_set_name,
                    self.params.stack_name,
                )
                self.set_failed(f"Change set '{self.params.change_set_name}' not found for stack '{self.params.stack_name}'")
            else:
                log.error(
                    "Error describing change set '{}': {} - {}",
                    self.params.change_set_name,
                    error_code,
                    error_message,
                )
                self.set_failed(f"Failed to describe change set '{self.params.change_set_name}': {error_message}")
            return

        except Exception as e:
            log.error(
                "Unexpected error describing change set '{}': {}",
                self.params.change_set_name,
                e,
            )
            self.set_failed(f"Unexpected error describing change set '{self.params.change_set_name}': {e}")
            return

        # Apply the change set
        self.set_running(f"Executing change set '{self.params.change_set_name}' on stack '{self.params.stack_name}'")

        try:
            log.info(
                "Executing change set {} on stack {}",
                self.params.change_set_name,
                self.params.stack_name,
            )

            cfn_client.execute_change_set(
                StackName=self.params.stack_name,
                ChangeSetName=self.params.change_set_name,
            )

            # Update state with execution info
            self.set_state("ChangeSetExecuted", True)
            self.set_state("ExecutionTime", util.get_current_timestamp())
            self.set_state("StackStatus", "UPDATE_IN_PROGRESS")

            log.info("Change set execution initiated for stack {}", self.params.stack_name)

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            error_message = e.response["Error"]["Message"]

            log.error(
                "Error executing change set '{}': {} - {}",
                self.params.change_set_name,
                error_code,
                error_message,
            )
            self.set_state("ApplicationResult", "FAILED")
            self.set_state("FailureReason", f"{error_code}: {error_message}")
            self.set_failed(f"Failed to execute change set '{self.params.change_set_name}': {error_message}")

        except Exception as e:
            log.error(
                "Unexpected error executing change set '{}': {}",
                self.params.change_set_name,
                e,
            )
            self.set_state("ApplicationResult", "FAILED")
            self.set_state("FailureReason", str(e))
            self.set_failed(f"Unexpected error executing change set '{self.params.change_set_name}': {e}")

        log.trace("ApplyChangeSetAction execution completed")

    def _check(self):
        """
        Check the status of the change set application operation.

        This method monitors the CloudFormation stack update progress and sets
        appropriate state based on the current status.
        """
        log.trace("Checking ApplyChangeSetAction")

        stack_id = self.get_state("StackId")

        if not stack_id:
            log.error("Stack ID not found in state - execute may not have run")
            self.set_failed("Stack ID not found in state - execute may not have run")
            return

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

        try:
            # Check stack status
            response = cfn_client.describe_stacks(StackName=stack_id)

            if not response.get("Stacks"):
                log.error("Stack '{}' not found", self.params.stack_name)
                self.set_failed(f"Stack '{self.params.stack_name}' not found")
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
                    f"Change set {self.params.change_set_name} applied successfully. {total_resources} resources affected."
                )
                log.info(
                    "Change set {} applied successfully to stack {}",
                    self.params.change_set_name,
                    self.params.stack_name,
                )

            elif stack_status in [
                "UPDATE_IN_PROGRESS",
                "CREATE_IN_PROGRESS",
                "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS",
            ]:
                # Still applying
                self.set_running(f"Change set {self.params.change_set_name} application in progress")

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
                self.set_running(f"Stack {self.params.stack_name} in status: {stack_status}")

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            error_message = e.response["Error"]["Message"]

            if error_code == "StackNotFoundException":
                log.error("Stack {} not found", self.params.stack_name)
                self.set_failed(f"Stack {self.params.stack_name} not found")
            else:
                log.error("Error checking stack status: {} - {}", error_code, error_message)
                self.set_failed(f"Error checking stack status: {error_message}")

        except Exception as e:
            log.error("Unexpected error checking stack status: {}", e)
            self.set_failed(f"Unexpected error checking stack status: {e}")

        log.trace("ApplyChangeSetAction check completed")

    def _unexecute(self):
        """
        Rollback the change set application operation.

        This method attempts to rollback the stack to its previous state by canceling
        the update or rolling back completed changes.
        """
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
                region=self.params.region,
                role=util.get_provisioning_role_arn(self.params.account),
            )
        except Exception as e:
            log.error("Failed to create CloudFormation client: {}", e)
            self.set_failed(f"Failed to create CloudFormation client: {e}")
            return

        try:
            # Check current stack status
            response = cfn_client.describe_stacks(StackName=stack_id)

            if not response.get("Stacks"):
                log.info("Stack '{}' no longer exists", self.params.stack_name)
                self.set_state("RollbackResult", "STACK_NOT_FOUND")
                self.set_complete(f"Stack '{self.params.stack_name}' no longer exists")
                return

            stack_info = response["Stacks"][0]
            stack_status = stack_info["StackStatus"]

            log.info("Current stack status for rollback: {}", stack_status)

            if stack_status in ["UPDATE_IN_PROGRESS", "CREATE_IN_PROGRESS"]:
                # Cancel the in-progress update
                log.info("Canceling in-progress stack update: {}", self.params.stack_name)

                cfn_client.cancel_update_stack(StackName=stack_id)

                self.set_state("RollbackInitiated", True)
                self.set_state("RollbackTime", util.get_current_timestamp())
                self.set_state("RollbackResult", "CANCEL_INITIATED")

                self.set_complete(f"Stack update cancellation initiated for {self.params.stack_name}")
                log.info("Stack update cancellation initiated for {}", self.params.stack_name)

            elif stack_status in ["UPDATE_COMPLETE", "CREATE_COMPLETE"]:
                # Stack update completed, attempt rollback
                log.info("Initiating stack rollback: {}", self.params.stack_name)

                # Note: CloudFormation doesn't have a direct rollback API for completed updates
                # This would typically require creating and applying a reverse change set
                log.warning("Stack rollback for completed updates requires manual intervention or reverse change set")

                self.set_state("RollbackResult", "MANUAL_INTERVENTION_REQUIRED")
                self.set_complete(f"Stack {self.params.stack_name} rollback requires manual intervention")

            elif stack_status in ["UPDATE_ROLLBACK_COMPLETE", "ROLLBACK_COMPLETE"]:
                # Already rolled back
                log.info(
                    "Stack {} is already in rollback complete state",
                    self.params.stack_name,
                )
                self.set_state("RollbackResult", "ALREADY_ROLLED_BACK")
                self.set_complete(f"Stack {self.params.stack_name} is already rolled back")

            else:
                log.warning(
                    "Stack {} is in status {} - rollback may not be applicable",
                    self.params.stack_name,
                    stack_status,
                )
                self.set_state("RollbackResult", f"NOT_APPLICABLE_{stack_status}")
                self.set_complete(f"Stack {self.params.stack_name} rollback not applicable for status: {stack_status}")

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            error_message = e.response["Error"]["Message"]

            if error_code == "StackNotFoundException":
                log.info("Stack {} not found during rollback", self.params.stack_name)
                self.set_state("RollbackResult", "STACK_NOT_FOUND")
                self.set_complete(f"Stack {self.params.stack_name} not found during rollback")
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
        """
        Cancel the change set application operation.

        This method attempts to cancel an in-progress change set application.
        """
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
        """
        Get detailed information about stack resources that were created, updated, or deleted.

        Args:
            cfn_client: CloudFormation client
            stack_id: ID of the stack

        Returns:
            tuple: (resources_created, resources_updated, resources_deleted)
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
    def generate_action_spec(cls, **kwargs) -> ApplyChangeSetActionSpec:
        return ApplyChangeSetActionSpec(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> ApplyChangeSetActionParams:
        return ApplyChangeSetActionParams(**kwargs)
