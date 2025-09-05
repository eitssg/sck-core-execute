"""Create and manage a CloudFormation change set.

- _execute: creates the change set and records identifiers
- _check: polls for status, captures changes, and finalizes results
- _unexecute: deletes the change set (best-effort rollback)
"""

from typing import Any
from pydantic import Field, model_validator
from botocore.exceptions import ClientError

import core_logging as log
import core_framework as util

from core_framework.models import ActionResource, ActionSpec, DeploymentDetails
from core_execute.actionlib.action import BaseAction
import core_helper.aws as aws


class CreateChangeSetActionSpec(ActionSpec):
    """Parameters for creating a CloudFormation change set.

    Attributes:
      account: AWS account ID used for the action.
      region: AWS region of the stack.
      stack_name: Target stack name.
      template_url: S3 URL of the CloudFormation template.
      change_set_name: Name for the change set.
      capabilities: Optional CloudFormation capabilities.
      parameters: Template parameter key/value pairs.
      tags: Optional key/value tags to apply.
    """

    stack_name: str = Field(
        ...,
        alias="StackName",
        description="The name of the stack to apply the change set",
    )
    template_url: str = Field(
        ...,
        alias="TemplateUrl",
        description="The template URL to use for the change set",
    )
    change_set_name: str = Field(
        ...,
        alias="ChangeSetName",
        description="The name of the change set to create",
    )
    capabilities: list[str] = Field(
        None,
        alias="Capabilities",
        description="The capabilities to enable for the stack (optional)",
    )
    parameters: dict = Field(
        default_factory=dict,
        alias="StackParameters",
        description="Parameters for the CloudFormation template",
    )
    tags: dict = Field(
        default_factory=dict,
        alias="Tags",
        description="Tags to apply to the change set",
    )

    @model_validator(mode="before")
    @classmethod
    def validate_model_before(cls, values: Any) -> dict[str, Any]:
        """Map legacy 'template'/'Template' keys to 'TemplateUrl' for compatibility."""
        if isinstance(values, dict):
            if not any(key in values for key in ["TemplateUrl", "template_url"]):
                template = values.pop("template") or values.pop("Template")
                if template is not None:
                    values["TemplateUrl"] = template

        return values


class CreateChangeSetActionResource(ActionResource):
    """Resource model for CreateChangeSetAction (normalizes kind/spec)."""

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Normalize incoming values and set canonical kind/spec."""
        if not isinstance(values, dict):
            return values

        values.pop("kind", None)
        values.pop("Kind", None)
        values["kind"] = "AWS::CreateChangeSet"

        spec = values.pop("spec", None) or values.pop("Spec", None)
        if isinstance(spec, dict):
            values["spec"] = spec
        elif isinstance(spec, CreateChangeSetActionSpec):
            values["spec"] = spec.model_dump()

        return values


class CreateChangeSetAction(BaseAction):
    """Create a CloudFormation change set for a stack.

    Behavior:
      - Supports cross-account via role assumption
      - Records identifiers, status, and changes to state/outputs
      - Idempotent if creation already started (uses saved ARN)

    State keys (subset):
      ChangeSetName, ChangeSetArn, ChangeSetId, ChangeSetStatus, StackId, StackExists

    Outputs (subset):
      ChangeSetArn, ChangeSetId, StackId, Changes, ChangesCount
    """

    def __init__(
        self,
        definition: ActionResource,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
        parent_action_name: str | None = None,
    ):
        """Initialize the action and validate parameters."""
        super().__init__(definition, context, deployment_details, parent_action_name)

        self.params = CreateChangeSetActionSpec(**definition.spec)

    def _resolve(self):
        """Render templates for account, region, names, parameters, and tags."""
        log.trace("Resolving CreateChangeSetAction")

        self.params.account = self.renderer.render_string(self.params.account, self.context)
        self.params.region = self.renderer.render_string(self.params.region, self.context)
        self.params.stack_name = self.renderer.render_string(self.params.stack_name, self.context)
        self.params.template_url = self.renderer.render_string(self.params.template_url, self.context)
        self.params.change_set_name = self.renderer.render_string(self.params.change_set_name, self.context)

        # Render stack parameters
        if self.params.parameters:
            rendered_params = {}
            for key, value in self.params.parameters.items():
                if isinstance(value, str):
                    rendered_params[key] = self.renderer.render_string(value, self.context)
                else:
                    rendered_params[key] = value
            self.params.parameters = rendered_params

        # Render tags
        if self.params.tags:
            rendered_tags = {}
            for key, value in self.params.tags.items():
                if isinstance(value, str):
                    rendered_tags[key] = self.renderer.render_string(value, self.context)
                else:
                    rendered_tags[key] = value
            self.params.tags = rendered_tags

        log.trace("CreateChangeSetAction resolved")

    def _execute(self):
        """Create the change set and set initial state/outputs.

        Sets failed status for missing parameters or CloudFormation client/operation errors.
        """
        log.trace("Executing CreateChangeSetAction")

        # Validate required parameters
        if not self.params.stack_name or self.params.stack_name == "":
            self.set_failed("StackName parameter is required")
            log.error("StackName parameter is required")
            return

        if not self.params.change_set_name or self.params.change_set_name == "":
            self.set_failed("ChangeSetName parameter is required")
            log.error("ChangeSetName parameter is required")
            return

        if not self.params.template_url or self.params.template_url == "":
            self.set_failed("TemplateUrl parameter is required")
            log.error("TemplateUrl parameter is required")
            return

        # Check if change set creation already started (idempotent execution)
        if self.get_state("ChangeSetCreationStarted") and self.get_state("ChangeSetArn"):
            log.info(
                "Change set creation already in progress for {}",
                self.params.change_set_name,
            )
            self.set_running(f"Change set creation already in progress for {self.params.change_set_name}")
            return

        # Set initial state information
        self.set_state("ChangeSetName", self.params.change_set_name)
        self.set_state("StackName", self.params.stack_name)
        self.set_state("Region", self.params.region)
        self.set_state("Account", self.params.account)
        self.set_state("ChangeSetCreationStarted", True)
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

        # Check if stack exists first for better logging
        stack_exists = False
        try:
            describe_response = cfn_client.describe_stacks(StackName=self.params.stack_name)

            if describe_response.get("Stacks"):
                stack_info = describe_response["Stacks"][0]
                stack_exists = True

                # Store stack information
                self.set_state("StackStatus", stack_info.get("StackStatus", ""))
                self.set_state("StackId", stack_info.get("StackId", ""))

                log.debug(
                    "Stack '{}' exists with status: {}",
                    self.params.stack_name,
                    stack_info.get("StackStatus", "UNKNOWN"),
                )

        except ClientError as e:
            if e.response["Error"]["Code"] in [
                "ValidationError",
                "StackNotFoundException",
            ]:
                stack_exists = False
                log.debug("Stack '{}' does not exist", self.params.stack_name)
            else:
                log.error(
                    "Error checking stack '{}': {}",
                    self.params.stack_name,
                    e.response["Error"]["Message"],
                )
                self.set_failed(f"Failed to check stack '{self.params.stack_name}': {e.response['Error']['Message']}")
                return
        except Exception as e:
            log.error("Unexpected error checking stack '{}': {}", self.params.stack_name, e)
            self.set_failed(f"Unexpected error checking stack '{self.params.stack_name}': {e}")
            return

        self.set_state("StackExists", stack_exists)

        # Determine change set type
        change_set_type = "UPDATE" if stack_exists else "CREATE"
        self.set_state("ChangeSetType", change_set_type)

        # Attempt to create the change set
        self.set_running(f"Creating change set '{self.params.change_set_name}' for stack '{self.params.stack_name}'")

        try:
            # Prepare change set parameters
            change_set_params = {
                "StackName": self.params.stack_name,
                "ChangeSetName": self.params.change_set_name,
                "TemplateURL": self.params.template_url,
                "ChangeSetType": change_set_type,
                "Capabilities": [
                    "CAPABILITY_IAM",
                    "CAPABILITY_NAMED_IAM",
                    "CAPABILITY_AUTO_EXPAND",
                ],
            }

            # Add stack parameters if provided
            if self.params.parameters:
                change_set_params["Parameters"] = [
                    {"ParameterKey": key, "ParameterValue": str(value)} for key, value in self.params.parameters.items()
                ]

            # Add tags if provided
            if self.params.tags:
                change_set_params["Tags"] = aws.transform_tag_hash(self.params.tags)

            # Create the change set
            log.info(
                "Creating change set {} for stack {}",
                self.params.change_set_name,
                self.params.stack_name,
            )

            response = cfn_client.create_change_set(**change_set_params)

            change_set_arn = response["Id"]
            change_set_id = response["Id"].split("/")[-1]
            stack_id = response["StackId"]

            # Update state with creation info
            self.set_state("ChangeSetArn", change_set_arn)
            self.set_state("ChangeSetId", change_set_id)
            self.set_state("StackId", stack_id)
            self.set_state("ChangeSetStatus", "CREATE_IN_PROGRESS")

            # Set outputs
            self.set_output("ChangeSetArn", change_set_arn)
            self.set_output("ChangeSetId", change_set_id)
            self.set_output("StackId", stack_id)

            log.info("Change set creation initiated: {}", change_set_arn)

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            error_message = e.response["Error"]["Message"]

            log.error(
                "Error creating change set '{}': {} - {}",
                self.params.change_set_name,
                error_code,
                error_message,
            )
            self.set_state("CreationResult", "FAILED")
            self.set_state("FailureReason", f"{error_code}: {error_message}")
            self.set_failed(f"Failed to create change set '{self.params.change_set_name}': {error_message}")

        except Exception as e:
            log.error(
                "Unexpected error creating change set '{}': {}",
                self.params.change_set_name,
                e,
            )
            self.set_state("CreationResult", "FAILED")
            self.set_state("FailureReason", str(e))
            self.set_failed(f"Unexpected error creating change set '{self.params.change_set_name}': {e}")

        log.trace("CreateChangeSetAction execution completed")

    def _check(self):
        """Check the change set status and update state/outputs accordingly."""
        log.trace("Checking CreateChangeSetAction")

        change_set_arn = self.get_state("ChangeSetArn")

        if not change_set_arn:
            log.error("Change set ARN not found in state - execute may not have run")
            self.set_failed("Change set ARN not found in state - execute may not have run")
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
            # Check change set status
            response = cfn_client.describe_change_set(ChangeSetName=change_set_arn)

            change_set_status = response["Status"]
            log.info("Change set status: {}", change_set_status)

            # Update state with current status
            self.set_state("ChangeSetStatus", change_set_status)

            if change_set_status == "CREATE_COMPLETE":
                # Change set created successfully
                changes = response.get("Changes", [])

                # Set comprehensive state outputs
                self.set_state("CreationCompleted", True)
                self.set_state("CompletionTime", util.get_current_timestamp())
                self.set_state("CreationResult", "SUCCESS")
                self.set_state("Changes", changes)
                self.set_state("ChangesCount", len(changes))

                # Set outputs for other actions
                self.set_output("CreationCompleted", True)
                self.set_output("CreationResult", "SUCCESS")
                self.set_output("Changes", changes)
                self.set_output("ChangesCount", len(changes))

                self.set_complete(f"Change set {self.params.change_set_name} created successfully with {len(changes)} changes")
                log.info(
                    "Change set {} created successfully with {} changes",
                    self.params.change_set_name,
                    len(changes),
                )

            elif change_set_status in ["CREATE_IN_PROGRESS", "CREATE_PENDING"]:
                # Still creating
                self.set_running(f"Change set {self.params.change_set_name} creation in progress")

            elif change_set_status in ["FAILED", "DELETE_COMPLETE"]:
                # Creation failed
                status_reason = response.get("StatusReason", "Unknown failure")
                log.error("Change set creation failed: {}", status_reason)

                self.set_state("CreationResult", "FAILED")
                self.set_state("FailureReason", status_reason)
                self.set_failed(f"Change set creation failed: {status_reason}")

            else:
                # Unknown status
                log.warning("Unknown change set status: {}", change_set_status)
                self.set_running(f"Change set {self.params.change_set_name} in unknown status: {change_set_status}")

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            error_message = e.response["Error"]["Message"]

            if error_code == "ChangeSetNotFoundException":
                log.error("Change set {} not found", self.params.change_set_name)
                self.set_failed(f"Change set {self.params.change_set_name} not found")
            else:
                log.error(
                    "Error checking change set status: {} - {}",
                    error_code,
                    error_message,
                )
                self.set_failed(f"Error checking change set status: {error_message}")

        except Exception as e:
            log.error("Unexpected error checking change set status: {}", e)
            self.set_failed(f"Unexpected error checking change set status: {e}")

        log.trace("CreateChangeSetAction check completed")

    def _unexecute(self):
        """Delete the created change set (best-effort rollback)."""
        log.trace("Unexecuting CreateChangeSetAction")

        change_set_arn = self.get_state("ChangeSetArn")

        if not change_set_arn:
            log.info("No change set to delete - nothing was created")
            self.set_state("RollbackResult", "NOT_FOUND")
            self.set_complete("No change set to delete - nothing was created")
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
            # Delete the change set
            log.info("Deleting change set: {}", change_set_arn)

            cfn_client.delete_change_set(ChangeSetName=change_set_arn)

            # Update state
            self.set_state("RollbackCompleted", True)
            self.set_state("RollbackTime", util.get_current_timestamp())
            self.set_state("RollbackResult", "SUCCESS")

            self.set_complete(f"Change set {self.params.change_set_name} deleted successfully")
            log.info("Change set {} deleted successfully", self.params.change_set_name)

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            error_message = e.response["Error"]["Message"]

            if error_code == "ChangeSetNotFoundException":
                # Change set already deleted
                log.info("Change set {} was already deleted", self.params.change_set_name)
                self.set_state("RollbackResult", "ALREADY_DELETED")
                self.set_complete(f"Change set {self.params.change_set_name} was already deleted")
            else:
                log.error("Error deleting change set: {} - {}", error_code, error_message)
                self.set_state("RollbackResult", "FAILED")
                self.set_state("FailureReason", f"{error_code}: {error_message}")
                self.set_failed(f"Error deleting change set: {error_message}")

        except Exception as e:
            log.error("Unexpected error deleting change set: {}", e)
            self.set_state("RollbackResult", "FAILED")
            self.set_state("FailureReason", str(e))
            self.set_failed(f"Unexpected error deleting change set: {e}")

        log.trace("CreateChangeSetAction unexecution completed")

    def _cancel(self):
        """Cancel an in-progress change set creation by deleting it if possible."""
        log.trace("Cancelling CreateChangeSetAction")

        change_set_status = self.get_state("ChangeSetStatus")

        if change_set_status in ["CREATE_IN_PROGRESS", "CREATE_PENDING"]:
            # Try to delete the in-progress change set
            log.info("Cancelling in-progress change set creation")
            self._unexecute()
        else:
            log.info("Change set not in cancellable state: {}", change_set_status)
            self.set_complete(f"Change set not in cancellable state: {change_set_status}")

        log.trace("CreateChangeSetAction cancellation completed")

    @classmethod
    def generate_action_resource(cls, **kwargs) -> CreateChangeSetActionResource:
        """Factory: create a typed CreateChangeSetActionResource."""
        return CreateChangeSetActionResource(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> CreateChangeSetActionSpec:
        """Factory: create typed CreateChangeSetActionSpec."""
        return CreateChangeSetActionSpec(**kwargs)
