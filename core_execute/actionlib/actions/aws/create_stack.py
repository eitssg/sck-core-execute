"""CloudFormation stack creation and management action.

Creates or updates a CloudFormation stack (via change sets), tracks progress,
captures outputs/metadata, and exposes them via action state/outputs.
"""

from typing import Any
from pydantic import Field, model_validator

import core_logging as log

from core_framework.models import DeploymentDetails, ActionResource, ActionSpec

import core_framework as util

from botocore.exceptions import ClientError

import core_helper.aws as aws

from core_execute.actionlib.action import BaseAction

CAPABILITITES = ["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM"]


class CreateStackActionSpec(ActionSpec):
    """Parameters for CloudFormation stack creation/update.

    Attributes:
      account: AWS account ID used for the operation.
      region: AWS region where the stack is managed.
      stack_name: CloudFormation stack name.
      template_url: S3 URL of the CloudFormation template.
      capabilities: List of capabilities to acknowledge (defaults to IAM capabilities).
      parameters: Key/value parameters passed to the template.
      on_failure: Behavior on create failure (DELETE, DO_NOTHING, ROLLBACK).
      timeout_in_minutes: Operation timeout in minutes.
      tags: Optional key/value tags to apply to the stack.
      stack_policy: Optional stack policy (dict) to apply during operations.
    """

    stack_name: str = Field(
        ...,
        alias="StackName",
        description="CloudFormation stack name",
    )
    template_url: str = Field(
        ...,
        alias="TemplateUrl",
        description="S3 URL of CloudFormation template",
    )
    capabilities: list[str] = Field(
        None,
        alias="Capabilities",
        description="CloudFormation capabilities (default: IAM capabilities)",
    )
    parameters: dict[str, Any] = Field(
        alias="StackParameters",
        description="CloudFormation template parameters",
        default={},
    )
    on_failure: str = Field(
        alias="OnFailure",
        description="Action on stack creation failure (DELETE, DO_NOTHING, ROLLBACK)",
        default="DELETE",
    )
    timeout_in_minutes: int = Field(
        alias="TimeoutInMinutes",
        description="Stack operation timeout in minutes",
        default=15,
    )
    tags: dict[str, str] | None = Field(
        None,
        alias="Tags",
        description="Stack tags",
    )
    stack_policy: dict | None = Field(
        None,
        alias="StackPolicy",
        description="Stack policy document (converted to JSON)",
    )

    @property
    def stack_policy_json(self):
        """Return the stack policy as a JSON string (or None if not set)."""
        if self.stack_policy is None:
            return None
        return util.to_json(self.stack_policy)

    @model_validator(mode="before")
    @classmethod
    def validate_model_before(cls, values: Any) -> dict[str, Any]:
        """Map legacy 'template'/'Template' fields to 'TemplateUrl' for compatibility."""
        if isinstance(values, dict):
            if not any(key in values for key in ["TemplateUrl", "template_url"]):
                template = values.pop("template") or values.pop("Template")
                if template is not None:
                    values["TemplateUrl"] = template

        return values


class CreateStackActionResource(ActionResource):
    """ActionResource wrapper for CreateStackAction with defaults.

    Normalizes 'kind' to 'AWS::CreateStack' and preserves/normalizes 'spec'.
    """

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Normalize incoming values and enforce canonical kind/spec structure."""
        if not isinstance(values, dict):
            return values

        if "Kind" in values:
            del values["Kind"]

        values["kind"] = "AWS::CreateStack"

        return values


class CreateStackAction(BaseAction[CreateStackActionSpec]):
    """CloudFormation stack creation and update action.

    Creates new CloudFormation stacks or updates existing ones using change sets.
    Automatically detects stack existence and chooses appropriate operation.

    Args:
      definition: Action specification with CloudFormation parameters.
      context: Jinja2 rendering context for variable substitution.
      deployment_details: Client/portfolio/app/branch/build information.

    Outputs:
      StackId: CloudFormation stack ID.
      StackName: Stack name.
      StackOperation: CREATE, UPDATE, or NO_UPDATE.
      StackStatus: Current CloudFormation stack status.
      {OutputKey}: All CloudFormation stack outputs.

    State Keys:
      StackName, TemplateUrl, Region, Account, StackId, StackExists,
      StackOperation, StackStatus, StackOutputCount, DriftDetectionId.
    """

    def __init__(
        self,
        definition: ActionResource,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        """Initialize CreateStackAction with validated parameters."""
        super().__init__(definition, context, deployment_details)

        # Validate the action parameters
        self.spec = CreateStackActionSpec(**definition.spec)

        if deployment_details.delivered_by:
            self.spec.tags["DeliveredBy"] = deployment_details.delivered_by

    def can_initialize(self) -> bool:
        """Return True if the action can be reinitialized (no CFN operation in progress)."""
        # Check if we're in middle of a critical operation
        stack_status = self.get_state("StackStatus")
        if stack_status in [
            "DELETE_IN_PROGRESS",
            "CREATE_IN_PROGRESS",
            "UPDATE_IN_PROGRESS",
        ]:
            log.warning(
                "Cannot reinitialize CreateStackAction - stack operation in progress: {}",
                stack_status,
            )
            return False

        return True

    def initialize(self) -> bool:
        """Reset stack-related state to allow a clean rerun without changing configuration.

        Returns:
            bool: True if initialization was successful, False otherwise.

        """
        log.info("Initializing CreateStackAction {} for rerun", self.name)

        # Call super to clear basic state
        super().initialize()

        # Clear CloudFormation-specific state
        cf_state_keys = [
            "StackId",
            "StackExists",
            "StackOperation",
            "StackStatus",
            "StackCreationStarted",
            "StackUpdateStarted",
            "StackOperationCompleted",
            "StackOperationFailed",
            "StackRolledBack",
            "ChangeSetId",
            "ChangeSetName",
            "StackEventsCount",
            "FailedStackEvents",
            "LatestStackEvent",
            "DriftDetectionId",
            "StackOutputCount",
            "StackResourceCount",
            "StackResourceTypes",
            "StackDescription",
            "StackCreationTime",
            "StackLastUpdatedTime",
            "StackTags",
        ]

        for key in cf_state_keys:
            state_key = f"{self.name}/{key}"
            if state_key in self.context:
                self.context.pop(state_key)
                log.trace("Cleared CloudFormation state: {}", state_key)

        # Clear any stack outputs that were previously saved
        output_keys_to_clear = []
        for key in self.context.keys():
            if key.startswith(f"{self.name}/") and not key.endswith("/StatusCode"):
                # Check if it looks like a CloudFormation output
                base_key = key.replace(f"{self.name}/", "")
                if base_key not in cf_state_keys and not base_key in [
                    "Account",
                    "Region",
                    "StackName",
                    "TemplateUrl",
                ]:
                    output_keys_to_clear.append(key)

        for key in output_keys_to_clear:
            self.context.pop(key)
            log.trace("Cleared CloudFormation output: {}", key)

        log.info("CreateStackAction {} reinitialized successfully", self.name)

        return True

    def can_execute(self) -> bool:
        """Validate required params and AWS connectivity prior to execution."""
        # Basic parameter validation
        if not self.spec.stack_name:
            log.error("Cannot execute - StackName is required")
            return False

        if not self.spec.template_url:
            log.error("Cannot execute - TemplateUrl is required")
            return False

        # Test AWS connectivity
        try:
            aws.cfn_client(
                region=self.spec.region,
                role_arn=util.get_provisioning_role_arn(self.spec.account),
            )
            return True
        except Exception as e:
            log.error("Cannot execute - AWS connectivity failed: {}", e)
            return False

    def _resolve(self):
        """Render template variables and coerce parameter types."""
        log.trace("Resolving CreateStackAction")

        self.spec.region = self.renderer.render_string(self.spec.region, self.context)
        self.spec.account = self.renderer.render_string(self.spec.account, self.context)
        self.spec.stack_name = self.renderer.render_string(self.spec.stack_name, self.context)
        self.spec.template_url = self.renderer.render_string(self.spec.template_url, self.context)
        self.spec.on_failure = self.renderer.render_string(self.spec.on_failure, self.context)

        # Handle timeout_in_minutes conversion
        timeout_rendered = self.renderer.render_string(str(self.spec.timeout_in_minutes), self.context)
        try:
            self.spec.timeout_in_minutes = int(timeout_rendered)
        except (ValueError, TypeError):
            log.warning("Invalid timeout value '{}', using default 15", timeout_rendered)
            self.spec.timeout_in_minutes = 15

        if self.spec.parameters:
            parameters_to_remove = []
            for parameter_key, parameter_value in self.spec.parameters.items():
                value = self.renderer.render_string(str(parameter_value), self.context)
                if value == "_NULL_":
                    parameters_to_remove.append(parameter_key)
                else:
                    self.spec.parameters[parameter_key] = value

            # Remove null parameters
            for key in parameters_to_remove:
                self.spec.parameters.pop(key)

        log.trace("Resolved CreateStackAction")

    def _execute(self):
        """Create a new stack or update an existing one based on current state."""
        log.trace("Executing CreateStackAction")

        # Validate required parameters
        if not self.spec.stack_name or self.spec.stack_name == "":
            self.set_failed("StackName parameter is required")
            log.error("StackName parameter is required")
            return

        if not self.spec.template_url or self.spec.template_url == "":
            self.set_failed("TemplateUrl parameter is required")
            log.error("TemplateUrl parameter is required")
            return

        # Set initial state information
        self.set_state("StackName", self.spec.stack_name)
        self.set_state("TemplateUrl", self.spec.template_url)
        self.set_state("Region", self.spec.region)
        self.set_state("Account", self.spec.account)

        # Set outputs for other actions to reference
        self.set_output("StackName", self.spec.stack_name)
        self.set_output("Region", self.spec.region)

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

        # Enhanced stack existence check for reruns
        stack_id = None
        stack_exists = False
        describe_stack_response = None

        try:
            describe_stack_response = cfn_client.describe_stacks(StackName=self.spec.stack_name)
            if describe_stack_response.get("Stacks"):
                stack_info = describe_stack_response["Stacks"][0]
                stack_id = stack_info["StackId"]
                stack_status = stack_info["StackStatus"]
                stack_exists = True

                self.set_state("StackId", stack_id)
                self.set_state("StackExists", True)
                self.set_state("StackStatus", stack_status)
                self.set_output("StackId", stack_id)

                log.info(
                    "Stack '{}' exists with status '{}' (ID: {})",
                    self.spec.stack_name,
                    stack_status,
                    stack_id,
                )

                # Check if stack is in a state that prevents operations
                if stack_status in ["DELETE_IN_PROGRESS", "DELETE_COMPLETE"]:
                    log.warning(
                        "Stack '{}' is in status '{}' - treating as non-existent for rerun",
                        self.spec.stack_name,
                        stack_status,
                    )
                    stack_exists = False
                    self.set_state("StackExists", False)

        except ClientError as e:
            if "does not exist" in e.response["Error"]["Message"]:
                stack_exists = False
                self.set_state("StackExists", False)
                log.info(
                    "Stack '{}' does not exist - will create new stack",
                    self.spec.stack_name,
                )
            else:
                log.error(
                    "Error describing stack '{}': {}",
                    self.spec.stack_name,
                    e.response["Error"]["Message"],
                )
                self.set_failed(f"Failed to describe stack '{self.spec.stack_name}': {e.response['Error']['Message']}")
                return
        except Exception as e:
            log.error("Unexpected error describing stack '{}': {}", self.spec.stack_name, e)
            self.set_failed(f"Unexpected error describing stack '{self.spec.stack_name}': {e}")
            return

        # Execute appropriate operation
        if stack_exists:
            self.__update_stack(cfn_client, stack_id, describe_stack_response)
        else:
            self.__create_stack(cfn_client)

        log.trace("CreateStackAction execution completed")

    def __create_stack(self, cfn_client):
        """Create a new CloudFormation stack after validating the template."""
        log.trace("Creating new stack '{}'", self.spec.stack_name)

        try:
            # Validate template before creating stack
            try:
                cfn_client.validate_template(TemplateURL=self.spec.template_url)
                log.debug("Template validation successful for: {}", self.spec.template_url)
            except ClientError as e:
                log.error("Template validation failed: {}", e.response["Error"]["Message"])
                self.set_failed(f"Template validation failed: {e.response['Error']['Message']}")
                return

            args = {
                "StackName": self.spec.stack_name,
                "TemplateURL": self.spec.template_url,
                "Capabilities": CAPABILITITES,
                "Parameters": aws.transform_stack_parameter_hash(self.spec.parameters),
                "OnFailure": self.spec.on_failure,
            }

            # Add optional parameters
            if self.spec.tags:
                args["Tags"] = aws.transform_tag_hash(self.spec.tags)
            if self.spec.timeout_in_minutes is not None:
                args["TimeoutInMinutes"] = self.spec.timeout_in_minutes
            if self.spec.stack_policy != "":
                args["StackPolicyBody"] = util.to_json(self.spec.stack_policy)

            log.debug(
                "Creating stack with parameters: StackName={}, TemplateURL={}, ParameterCount={}, TagCount={}",
                self.spec.stack_name,
                self.spec.template_url,
                len(self.spec.parameters),
                len(self.spec.tags),
            )

            cfn_response = cfn_client.create_stack(**args)
            stack_id = cfn_response["StackId"]

            # Set comprehensive state outputs
            self.set_state("StackId", stack_id)
            self.set_state("StackOperation", "CREATE")
            self.set_state("StackCreationStarted", True)
            self.set_state("StackCreationTime", util.get_current_timestamp())

            # Set outputs for other actions
            self.set_output("StackId", stack_id)
            self.set_output("StackOperation", "CREATE")

            self.set_running(f"Creating new stack '{self.spec.stack_name}'")
            log.debug("Stack creation initiated with ID: {}", stack_id)

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            error_message = e.response["Error"]["Message"]

            # Handle specific CloudFormation errors
            if error_code == "AlreadyExistsException":
                log.warning(
                    "Stack '{}' already exists, will attempt update",
                    self.spec.stack_name,
                )
                self.set_state("StackExists", True)
                self.set_failed(f"Stack '{self.spec.stack_name}' already exists")
            elif error_code == "InsufficientCapabilitiesException":
                log.error("Insufficient capabilities for stack creation: {}", error_message)
                self.set_failed(f"Insufficient capabilities: {error_message}")
            elif error_code == "LimitExceededException":
                log.error("CloudFormation limits exceeded: {}", error_message)
                self.set_failed(f"CloudFormation limits exceeded: {error_message}")
            else:
                log.error(
                    "Failed to create stack '{}': {} - {}",
                    self.spec.stack_name,
                    error_code,
                    error_message,
                )
                self.set_failed(f"Failed to create stack '{self.spec.stack_name}': {error_message}")

        except Exception as e:
            log.error("Unexpected error creating stack '{}': {}", self.spec.stack_name, e)
            self.set_failed(f"Unexpected error creating stack '{self.spec.stack_name}': {e}")

        log.trace("Stack creation initiated")

    def __update_stack(self, cfn_client: Any, stack_id: str, describe_stack_response: dict):
        """Update an existing CloudFormation stack using a change set.

        Args:
          cfn_client: boto3 CloudFormation client.
          stack_id: Stack ID or ARN.
          describe_stack_response: Result of describe_stacks for the target stack.
        """
        try:
            log.trace("Updating existing stack '{}'", self.spec.stack_name)

            # Create a change set first for safer updates
            change_set_name = f"update-{util.get_current_timestamp()}"

            args = {
                "StackName": stack_id,
                "TemplateURL": self.spec.template_url,
                "Capabilities": CAPABILITITES,
                "Parameters": aws.transform_stack_parameter_hash(self.spec.parameters or {}),
                "ChangeSetName": change_set_name,
            }
            if self.spec.tags:
                args["Tags"] = aws.transform_tag_hash(self.spec.tags)
            if self.spec.stack_policy:
                args["StackPolicyBody"] = util.to_json(self.spec.stack_policy)

            # Create and execute change set
            try:
                change_set_response = cfn_client.create_change_set(**args)
                change_set_id = change_set_response.get("Id")

                self.set_state("ChangeSetId", change_set_id)
                self.set_state("ChangeSetName", change_set_name)

                log.debug("Created change set '{}' for stack update", change_set_name)

                # Wait briefly for change set to be created
                import time

                time.sleep(2)

                # Describe the change set to see what changes
                change_set_details = cfn_client.describe_change_set(StackName=stack_id, ChangeSetName=change_set_name)

                changes = change_set_details.get("Changes", [])
                if not changes:
                    # No changes detected
                    log.debug(
                        "No changes detected in change set for stack '{}'",
                        self.spec.stack_name,
                    )

                    # Delete the empty change set
                    cfn_client.delete_change_set(StackName=stack_id, ChangeSetName=change_set_name)

                    self.set_state("StackOperation", "NO_UPDATE")
                    self.set_state("NoUpdatesRequired", True)
                    self.set_output("StackOperation", "NO_UPDATE")
                    self.set_complete("No changes required")
                    self.__save_stack_outputs(describe_stack_response)
                    return

                # Log the changes and execute
                self.set_state("ChangeCount", len(changes))
                log.debug("Change set contains {} changes", len(changes))

                # Execute the change set
                cfn_client.execute_change_set(StackName=stack_id, ChangeSetName=change_set_name)

                # Set comprehensive state outputs
                self.set_state("StackOperation", "UPDATE")
                self.set_state("StackUpdateStarted", True)
                self.set_state("StackUpdateTime", util.get_current_timestamp())

                # Set outputs for other actions
                self.set_output("StackOperation", "UPDATE")

                self.set_running(f"Updating existing stack '{self.spec.stack_name}'")
                log.debug("Stack update initiated via change set for: {}", stack_id)

            except ClientError as cs_error:
                if (
                    "No updates" in cs_error.response["Error"]["Message"]
                    or "didn't contain changes" in cs_error.response["Error"]["Message"]
                ):
                    log.debug("No updates required for stack '{}'", self.spec.stack_name)
                    self.set_state("StackOperation", "NO_UPDATE")
                    self.set_state("NoUpdatesRequired", True)
                    self.set_output("StackOperation", "NO_UPDATE")
                    self.set_complete("No changes required")
                    self.__save_stack_outputs(describe_stack_response)
                else:
                    raise cs_error

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            error_message = e.response["Error"]["Message"]
            log.error(
                "Error updating stack '{}': {} - {}",
                self.spec.stack_name,
                error_code,
                error_message,
            )
            self.set_failed(f"Failed to update stack '{self.spec.stack_name}': {error_message}")
        except Exception as e:
            log.error("Unexpected error updating stack '{}': {}", self.spec.stack_name, e)
            self.set_failed(f"Unexpected error updating stack '{self.spec.stack_name}': {e}")

        log.trace("Stack update initiated")

    def _check(self):
        """Monitor CloudFormation stack operation progress and capture results."""
        log.trace("Checking CreateStackAction")

        stack_id = self.get_state("StackId")
        if not stack_id:
            self.set_failed("No stack ID found in state")
            return

        # Obtain a CloudFormation client
        try:
            cfn_client = aws.cfn_client(
                region=self.spec.region,
                role_arn=util.get_provisioning_role_arn(self.spec.account),
            )
        except Exception as e:
            log.error("Failed to create CloudFormation client for status check: {}", e)
            self.set_failed(f"Failed to create CloudFormation client: {e}")
            return

        # Check stack status and handle completion
        try:
            describe_stack_response = cfn_client.describe_stacks(StackName=stack_id)
            if not describe_stack_response.get("Stacks"):
                self.set_failed(f"Stack '{stack_id}' not found")
                return

            stack_info = describe_stack_response["Stacks"][0]
            stack_status = stack_info["StackStatus"]

            # Update state with current status
            self.set_state("StackStatus", stack_status)
            self.set_state("LastChecked", util.get_current_timestamp())
            self.set_state("StackStatusReason", stack_info.get("StackStatusReason", ""))
            self.set_output("StackStatus", stack_status)

            # Status classification
            creation_complete_states = [
                "CREATE_COMPLETE",
                "UPDATE_COMPLETE",
                "IMPORT_COMPLETE",
            ]
            creation_failed_states = [
                "CREATE_FAILED",
                "UPDATE_FAILED",
                "DELETE_FAILED",
                "ROLLBACK_FAILED",
                "UPDATE_ROLLBACK_FAILED",
                "IMPORT_ROLLBACK_FAILED",
            ]
            rollback_states = [
                "ROLLBACK_COMPLETE",
                "UPDATE_ROLLBACK_COMPLETE",
                "IMPORT_ROLLBACK_COMPLETE",
                "ROLLBACK_IN_PROGRESS",
                "UPDATE_ROLLBACK_IN_PROGRESS",
            ]
            in_progress_states = [
                "CREATE_IN_PROGRESS",
                "UPDATE_IN_PROGRESS",
                "DELETE_IN_PROGRESS",
                "REVIEW_IN_PROGRESS",
                "IMPORT_IN_PROGRESS",
            ]

            if stack_status in creation_complete_states:
                # Stack operation completed successfully
                self.set_state("StackOperationCompleted", True)
                self.set_state("CompletionTime", util.get_current_timestamp())
                self.set_output("StackOperationCompleted", True)

                # Capture stack drift detection if available
                self._check_stack_drift(cfn_client, stack_id)

                self.set_complete("Stack operation completed successfully")
                self.__save_stack_outputs(describe_stack_response)

            elif stack_status in creation_failed_states:
                # Stack operation failed
                failure_reason = stack_info.get("StackStatusReason", "Unknown failure")
                self.set_state("StackOperationFailed", True)
                self.set_state("FailureReason", failure_reason)

                # Capture failure details
                self._capture_stack_events(cfn_client, stack_id, failed=True)

                self.set_failed(f"Stack operation failed: {stack_status} - {failure_reason}")

            elif stack_status in rollback_states:
                # Stack rolled back
                rollback_reason = stack_info.get("StackStatusReason", "Stack rolled back")
                self.set_state("StackOperationFailed", True)
                self.set_state("StackRolledBack", True)
                self.set_state("RollbackReason", rollback_reason)

                self._capture_stack_events(cfn_client, stack_id, failed=True)

                self.set_failed(f"Stack rolled back: {stack_status} - {rollback_reason}")

            elif stack_status in in_progress_states:
                # Operation still in progress
                progress_info = stack_info.get("StackStatusReason", "")
                self.set_running(f"Stack operation in progress: {stack_status}")

                # Capture intermediate events
                self._capture_stack_events(cfn_client, stack_id, failed=False)

                log.debug("Stack status: {} - {}", stack_status, progress_info)
            else:
                # Unknown status
                log.warning("Unknown stack status: {}", stack_status)
                self.set_running(f"Stack in unknown state: {stack_status}")

        except ClientError as e:
            log.error(
                "Failed to describe stack '{}': {}",
                stack_id,
                e.response["Error"]["Message"],
            )
            self.set_failed(f"Failed to describe stack '{stack_id}': {e.response['Error']['Message']}")
            return
        except Exception as e:
            log.error("Unexpected error describing stack '{}': {}", stack_id, e)
            self.set_failed(f"Unexpected error describing stack '{stack_id}': {e}")
            return

        log.trace("CreateStackAction check completed")

    def _unexecute(self):
        """Initiate deletion of the stack as a best-effort rollback."""
        log.trace("Unexecuting CreateStackAction")

        stack_id = self.get_state("StackId")
        if not stack_id:
            log.debug("No stack ID found in state - nothing to rollback")
            self.set_complete("No stack to rollback")
            return

        try:
            cfn_client = aws.cfn_client(
                region=self.spec.region,
                role_arn=util.get_provisioning_role_arn(self.spec.account),
            )
        except Exception as e:
            log.error("Failed to create CloudFormation client for rollback: {}", e)
            self.set_failed(f"Failed to create CloudFormation client for rollback: {e}")
            return

        try:
            cfn_client.delete_stack(StackName=stack_id)
            log.debug("Initiated deletion of stack '{}'", stack_id)

            self.set_state("StackDeletionStarted", True)
            self.set_running(f"Deleting stack '{self.spec.stack_name}'")

        except ClientError as e:
            log.error(
                "Failed to delete stack '{}': {}",
                stack_id,
                e.response["Error"]["Message"],
            )
            self.set_failed(f"Failed to delete stack '{stack_id}': {e.response['Error']['Message']}")
        except Exception as e:
            log.error("Unexpected error deleting stack '{}': {}", stack_id, e)
            self.set_failed(f"Unexpected error deleting stack '{stack_id}': {e}")

    def _cancel(self):
        """Cancel an in-progress CloudFormation stack update if possible."""
        log.trace("Cancelling CreateStackAction")

        stack_id = self.get_state("StackId")
        if not stack_id:
            self.set_complete("No stack operation to cancel")
            return

        try:
            cfn_client = aws.cfn_client(
                region=self.spec.region,
                role_arn=util.get_provisioning_role_arn(self.spec.account),
            )

            cfn_client.cancel_update_stack(StackName=stack_id)
            log.debug("Cancelled stack update for '{}'", stack_id)

            self.set_state("StackOperationCancelled", True)
            self.set_complete("Stack operation cancelled")

        except ClientError as e:
            if "No updates are currently in progress" in e.response["Error"]["Message"]:
                self.set_complete("No stack operation in progress to cancel")
            else:
                log.warning(
                    "Failed to cancel stack operation '{}': {}",
                    stack_id,
                    e.response["Error"]["Message"],
                )
                self.set_complete("Stack operation cancellation failed")
        except Exception as e:
            log.warning("Unexpected error cancelling stack operation '{}': {}", stack_id, e)
            self.set_complete("Stack operation cancellation failed")

    def _capture_stack_events(self, cfn_client, stack_id: str, failed: bool = False):
        """Capture recent CloudFormation stack events for troubleshooting.

        Args:
          cfn_client: boto3 CloudFormation client.
          stack_id: Stack ID or ARN.
          failed: If True, store only recent failed events.
        """
        try:
            events_response = cfn_client.describe_stack_events(StackName=stack_id)
            events = events_response.get("StackEvents", [])

            # Store recent events count
            self.set_state("StackEventsCount", len(events))

            # Capture failed events for troubleshooting
            if failed:
                failed_events = []
                for event in events[:10]:  # Last 10 events
                    if "FAILED" in event.get("ResourceStatus", ""):
                        failed_event = {
                            "ResourceType": event.get("ResourceType", ""),
                            "LogicalResourceId": event.get("LogicalResourceId", ""),
                            "ResourceStatus": event.get("ResourceStatus", ""),
                            "ResourceStatusReason": event.get("ResourceStatusReason", ""),
                            "Timestamp": (event.get("Timestamp", "").isoformat() if event.get("Timestamp") else ""),
                        }
                        failed_events.append(failed_event)

                        # Log individual failures
                        log.error(
                            "Stack resource failed: {} ({}) - {} - {}",
                            event.get("LogicalResourceId"),
                            event.get("ResourceType"),
                            event.get("ResourceStatus"),
                            event.get("ResourceStatusReason", "No reason provided"),
                        )

                if failed_events:
                    self.set_state("FailedStackEvents", failed_events)

            # Always capture the latest event for progress tracking
            if events:
                latest_event = events[0]
                self.set_state(
                    "LatestStackEvent",
                    {
                        "ResourceType": latest_event.get("ResourceType", ""),
                        "ResourceStatus": latest_event.get("ResourceStatus", ""),
                        "LogicalResourceId": latest_event.get("LogicalResourceId", ""),
                        "Timestamp": (latest_event.get("Timestamp", "").isoformat() if latest_event.get("Timestamp") else ""),
                    },
                )

        except Exception as e:
            log.warning("Failed to capture stack events for '{}': {}", stack_id, e)

    def __save_stack_outputs(self, describe_stack_response: dict):
        """Persist CloudFormation stack outputs and metadata to action state/outputs.

        Args:
          describe_stack_response: Response dict from describe_stacks.
        """
        try:
            stack_info = describe_stack_response["Stacks"][0]

            # Save stack outputs
            outputs = stack_info.get("Outputs", [])
            output_count = len(outputs)

            for output in outputs:
                output_key = output.get("OutputKey")
                output_value = output.get("OutputValue")
                output_description = output.get("Description", "")

                if output_key and output_value is not None:
                    self.set_output(output_key, output_value)
                    log.trace(
                        "Saved stack output: {} = {} ({})",
                        output_key,
                        output_value,
                        output_description,
                    )

            self.set_state("StackOutputCount", output_count)
            self.set_output("StackOutputCount", output_count)

            # Save additional stack metadata
            self.set_state("StackDescription", stack_info.get("Description", ""))
            self.set_state(
                "StackCreationTime",
                (stack_info.get("CreationTime", "").isoformat() if stack_info.get("CreationTime") else ""),
            )
            self.set_state(
                "StackLastUpdatedTime",
                (stack_info.get("LastUpdatedTime", "").isoformat() if stack_info.get("LastUpdatedTime") else ""),
            )

            # Capture stack tags for reference
            stack_tags = stack_info.get("Tags", [])
            if stack_tags:
                tag_dict = {tag["Key"]: tag["Value"] for tag in stack_tags}
                self.set_state("StackTags", tag_dict)

            # Get resource summary
            self._capture_resource_summary()

            log.debug("Saved {} stack outputs and metadata", output_count)

        except (KeyError, IndexError, TypeError) as e:
            log.warning("Error saving stack outputs: {}", e)

    def _capture_resource_summary(self):
        """Capture a simple count of resources and types present in the stack."""
        try:
            cfn_client = aws.cfn_client(
                region=self.spec.region,
                role_arn=util.get_provisioning_role_arn(self.spec.account),
            )

            stack_id = self.get_state("StackId")
            if not stack_id:
                return

            resources_response = cfn_client.list_stack_resources(StackName=stack_id)
            resources = resources_response.get("StackResourceSummaries", [])

            # Count resources by type
            resource_counts = {}
            for resource in resources:
                resource_type = resource.get("ResourceType", "Unknown")
                resource_counts[resource_type] = resource_counts.get(resource_type, 0) + 1

            self.set_state("StackResourceCount", len(resources))
            self.set_state("StackResourceTypes", resource_counts)
            self.set_output("StackResourceCount", len(resources))

            log.debug(
                "Stack contains {} resources across {} types",
                len(resources),
                len(resource_counts),
            )

        except Exception as e:
            log.warning("Failed to capture resource summary: {}", e)

    def _check_stack_drift(self, cfn_client, stack_id: str):
        """Kick off CloudFormation stack drift detection (best-effort)."""
        try:
            # Initiate drift detection
            drift_response = cfn_client.detect_stack_drift(StackName=stack_id)
            drift_detection_id = drift_response.get("StackDriftDetectionId")

            if drift_detection_id:
                self.set_state("DriftDetectionId", drift_detection_id)
                log.debug("Initiated stack drift detection: {}", drift_detection_id)

        except Exception as e:
            log.warning("Failed to initiate drift detection for stack '{}': {}", stack_id, e)

    @classmethod
    def generate_action_resource(cls, **kwargs) -> CreateStackActionResource:
        """Factory: create a typed CreateStackActionResource."""
        return CreateStackActionResource(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> CreateStackActionSpec:
        """Factory: create a typed CreateStackActionSpec."""
        return CreateStackActionSpec(**kwargs)
