"""Deploy a Cloudformation stack"""

from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator

import core_logging as log

from core_framework.models import DeploymentDetails, ActionSpec, ActionParams

import core_framework as util

from botocore.exceptions import ClientError

import core_helper.aws as aws

from core_execute.actionlib.action import BaseAction

CAPABILITITES = ["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM"]


class CreateStackActionParams(ActionParams):
    """
    Parameters for the CreateStackAction.

    :param account: The account to use for the action (required)
    :type account: str
    :param region: The region to create the stack in (required)
    :type region: str
    :param stack_name: The name of the stack to create (required)
    :type stack_name: str
    :param template_url: The URL of the CloudFormation template (required)
    :type template_url: str
    :param stack_parameters: The parameters to pass to the stack (optional)
    :type stack_parameters: dict[str, Any]
    :param on_failure: The action to take on failure (optional, defaults to DELETE)
    :type on_failure: str
    :param timeout_in_minutes: The time to wait for the stack to complete (optional, defaults to 15)
    :type timeout_in_minutes: int
    :param tags: The tags to apply to the stack (optional)
    :type tags: dict[str, str]
    :param stack_policy: A policy statement to use within the stack deployment (optional)
    :type stack_policy: str
    """

    stack_name: str = Field(
        ...,
        alias="StackName",
        description="The name of the stack to create (required)",
    )
    template_url: str = Field(
        ...,
        alias="TemplateUrl",
        description="The URL of the CloudFormation template (required)",
    )
    stack_parameters: dict[str, Any] = Field(
        default_factory=dict,
        alias="StackParameters",
        description="The parameters to pass to the stack (optional)",
    )
    on_failure: str = Field(
        default="DELETE",
        alias="OnFailure",
        description="The action to take on failure (optional)",
    )
    timeout_in_minutes: int = Field(
        default=15,
        alias="TimeoutInMinutes",
        description="The time to wait for the stack to complete (optional)",
    )
    tags: dict[str, str] | None = Field(
        default_factory=dict,
        alias="Tags",
        description="The tags to apply to the stack (optional)",
    )
    stack_policy: dict | None = Field(
        ...,
        alias="StackPolicy",
        description="A policy statement to use within the stack deployment as needed (optional) (converted to JSON)",
    )

    @property
    def stack_policy_json(self):
        if self.stack_policy is None:
            return None
        return util.to_json(self.stack_policy)

    @model_validator(mode="before")
    @classmethod
    def validate_model_before(cls, values: Any) -> dict[str, Any]:
        if isinstance(values, dict):
            if not any(key in values for key in ["TemplateUrl", "template_url"]):
                template = values.pop("template") or values.pop("Template")
                if template is not None:
                    values["TemplateUrl"] = template

        return values


class CreateStackActionSpec(ActionSpec):
    """
    Generate the action definition for CreateStackAction.

    This class provides default values and validation for CreateStackAction parameters.

    :param values: Dictionary of action specification values
    :type values: dict[str, Any]
    :return: Validated action specification values
    :rtype: dict[str, Any]
    """

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """
        Validate the parameters for the CreateStackActionSpec.

        :param values: Input values for validation
        :type values: dict[str, Any]
        :return: Validated and potentially modified values
        :rtype: dict[str, Any]
        """
        if not (values.get("name") or values.get("Name")):
            values["name"] = "action-aws-createstack-name"
        if not (values.get("kind") or values.get("Kind")):
            values["kind"] = "AWS::CreateStack"
        if not (values.get("depends_on") or values.get("DependsOn")):
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Params")):
            values["params"] = {
                "account": "",
                "region": "",
                "stack_name": "",
                "template_url": "",
                "stack_parameters": {},
                "on_failure": "DELETE",
                "timeout_in_minutes": 15,
                "tags": {},
                "stack_policy": "",
            }
        return values


class CreateStackAction(BaseAction):
    """
    Create a CloudFormation stack.

    This action will create or update a CloudFormation stack and wait for the operation
    to complete before returning. It handles both creation and updates automatically.

    :param definition: The action specification containing configuration details
    :type definition: ActionSpec
    :param context: The Jinja2 rendering context containing all variables
    :type context: dict[str, Any]
    :param deployment_details: Client/portfolio/app/branch/build information
    :type deployment_details: DeploymentDetails

    .. rubric:: Parameters

    :Name: Enter a name to define this action instance
    :Kind: Use the value ``AWS::CreateStack``
    :Params.Account: The account where CloudFormation is located (required)
    :Params.Region: The region where CloudFormation is located (required)
    :Params.StackName: The name of the stack to create (required)
    :Params.TemplateUrl: The URL of the CloudFormation template (required)
    :Params.StackParameters: The parameters to pass to the stack (optional)
    :Params.OnFailure: The action to take on failure (optional, defaults to DELETE)
    :Params.TimeoutInMinutes: The time to wait for the stack to complete (optional, defaults to 15)
    :Params.Tags: The tags to apply to the stack (optional)
    :Params.StackPolicy: A policy statement to use within the stack deployment (optional)

    .. rubric:: ActionSpec Example

    .. code-block:: yaml

        - Name: action-aws-createstack-name
          Kind: "AWS::CreateStack"
          Params:
            Account: "154798051514"
            Region: "ap-southeast-1"
            StackName: "my-application-stack"
            TemplateUrl: "s3://my-bucket/my-template.yaml"
            StackParameters:
              Build: "ver1.0"
              Environment: "production"
            Tags:
              App: "My application"
              Environment: "production"
            TimeoutInMinutes: 15
          Scope: "build"

    .. note::
        The action automatically detects if a stack exists and performs updates instead of creation.

    .. warning::
        Stack creation/update can take significant time depending on resources being deployed.
    """

    def __init__(
        self,
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        # Validate the action parameters
        self.params = CreateStackActionParams(**definition.params)

        if deployment_details.delivered_by:
            self.params.tags["DeliveredBy"] = deployment_details.delivered_by

    def _resolve(self):
        """
        Resolve template variables in action parameters.

        This method renders Jinja2 templates in the action parameters using the current context.
        """
        log.trace("Resolving CreateStackAction")

        self.params.region = self.renderer.render_string(self.params.region, self.context)  # Fixed: was self.params.r
        self.params.account = self.renderer.render_string(self.params.account, self.context)
        self.params.stack_name = self.renderer.render_string(self.params.stack_name, self.context)
        self.params.template_url = self.renderer.render_string(self.params.template_url, self.context)
        self.params.on_failure = self.renderer.render_string(self.params.on_failure, self.context)

        # Handle timeout_in_minutes conversion
        timeout_rendered = self.renderer.render_string(str(self.params.timeout_in_minutes), self.context)
        try:
            self.params.timeout_in_minutes = int(timeout_rendered)
        except (ValueError, TypeError):
            log.warning("Invalid timeout value '{}', using default 15", timeout_rendered)
            self.params.timeout_in_minutes = 15

        if self.params.stack_parameters:
            parameters_to_remove = []
            for parameter_key, parameter_value in self.params.stack_parameters.items():
                value = self.renderer.render_string(str(parameter_value), self.context)
                if value == "_NULL_":
                    parameters_to_remove.append(parameter_key)
                else:
                    self.params.stack_parameters[parameter_key] = value

            # Remove null parameters
            for key in parameters_to_remove:
                self.params.stack_parameters.pop(key)

        log.trace("Resolved CreateStackAction")

    def _execute(self):
        """
        Execute the CloudFormation stack operation.

        This method creates or updates a CloudFormation stack and sets appropriate
        state outputs for tracking.

        :raises: Sets action to failed if stack name/template is missing or CloudFormation operation fails
        """
        log.trace("Executing CreateStackAction")

        # Validate required parameters
        if not self.params.stack_name or self.params.stack_name == "":
            self.set_failed("StackName parameter is required")
            log.error("StackName parameter is required")
            return

        if not self.params.template_url or self.params.template_url == "":
            self.set_failed("TemplateUrl parameter is required")
            log.error("TemplateUrl parameter is required")
            return

        # Set initial state information
        self.set_state("StackName", self.params.stack_name)
        self.set_state("TemplateUrl", self.params.template_url)
        self.set_state("Region", self.params.region)
        self.set_state("Account", self.params.account)

        # Set outputs for other actions to reference
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

        # Determine if the stack already exists
        stack_id = None
        stack_exists = False
        describe_stack_response = None

        try:
            describe_stack_response = cfn_client.describe_stacks(StackName=self.params.stack_name)
            if describe_stack_response.get("Stacks"):
                stack_id = describe_stack_response["Stacks"][0]["StackId"]
                stack_exists = True
                self.set_state("StackId", stack_id)
                self.set_state("StackExists", True)
                self.set_output("StackId", stack_id)
                log.debug(
                    "Stack '{}' already exists with ID: {}",
                    self.params.stack_name,
                    stack_id,
                )
        except ClientError as e:
            if "does not exist" in e.response["Error"]["Message"]:
                stack_exists = False
                self.set_state("StackExists", False)
                log.debug(
                    "Stack '{}' does not exist - will create new stack",
                    self.params.stack_name,
                )
            else:
                log.error(
                    "Error describing stack '{}': {}",
                    self.params.stack_name,
                    e.response["Error"]["Message"],
                )
                self.set_failed(f"Failed to describe stack '{self.params.stack_name}': {e.response['Error']['Message']}")
                return
        except Exception as e:
            log.error("Unexpected error describing stack '{}': {}", self.params.stack_name, e)
            self.set_failed(f"Unexpected error describing stack '{self.params.stack_name}': {e}")
            return

        # Stack exists, attempt an update, else create a new one
        if stack_exists:
            self.__update_stack(cfn_client, stack_id, describe_stack_response)
        else:
            self.__create_stack(cfn_client)

        log.trace("CreateStackAction execution completed")

    def __create_stack(self, cfn_client):
        """
        Create a new CloudFormation stack with enhanced error handling.

        :param cfn_client: The CloudFormation boto3 client
        :type cfn_client: boto3.client
        """
        log.trace("Creating new stack '{}'", self.params.stack_name)

        try:
            # Validate template before creating stack
            try:
                cfn_client.validate_template(TemplateURL=self.params.template_url)
                log.debug("Template validation successful for: {}", self.params.template_url)
            except ClientError as e:
                log.error("Template validation failed: {}", e.response["Error"]["Message"])
                self.set_failed(f"Template validation failed: {e.response['Error']['Message']}")
                return

            args = {
                "StackName": self.params.stack_name,
                "TemplateURL": self.params.template_url,
                "Capabilities": CAPABILITITES,
                "Parameters": aws.transform_stack_parameter_hash(self.params.stack_parameters),
                "OnFailure": self.params.on_failure,
            }

            # Add optional parameters
            if self.params.tags:
                args["Tags"] = aws.transform_tag_hash(self.params.tags)
            if self.params.timeout_in_minutes is not None:
                args["TimeoutInMinutes"] = self.params.timeout_in_minutes
            if self.params.stack_policy != "":
                args["StackPolicyBody"] = util.to_json(self.params.stack_policy)

            # Enhanced logging of stack creation parameters
            log.debug(
                "Creating stack with parameters: StackName={}, TemplateURL={}, ParameterCount={}, TagCount={}",
                self.params.stack_name,
                self.params.template_url,
                len(self.params.stack_parameters),
                len(self.params.tags),
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

            self.set_running(f"Creating new stack '{self.params.stack_name}'")
            log.debug("Stack creation initiated with ID: {}", stack_id)

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            error_message = e.response["Error"]["Message"]

            # Handle specific CloudFormation errors
            if error_code == "AlreadyExistsException":
                log.warning(
                    "Stack '{}' already exists, will attempt update",
                    self.params.stack_name,
                )
                # This shouldn't happen due to our existence check, but handle gracefully
                self.set_state("StackExists", True)
                self.set_failed(f"Stack '{self.params.stack_name}' already exists")
            elif error_code == "InsufficientCapabilitiesException":
                log.error("Insufficient capabilities for stack creation: {}", error_message)
                self.set_failed(f"Insufficient capabilities: {error_message}")
            elif error_code == "LimitExceededException":
                log.error("CloudFormation limits exceeded: {}", error_message)
                self.set_failed(f"CloudFormation limits exceeded: {error_message}")
            else:
                log.error(
                    "Failed to create stack '{}': {} - {}",
                    self.params.stack_name,
                    error_code,
                    error_message,
                )
                self.set_failed(f"Failed to create stack '{self.params.stack_name}': {error_message}")

        except Exception as e:
            log.error("Unexpected error creating stack '{}': {}", self.params.stack_name, e)
            self.set_failed(f"Unexpected error creating stack '{self.params.stack_name}': {e}")

        log.trace("Stack creation initiated")

    def __update_stack(self, cfn_client: Any, stack_id: str, describe_stack_response: dict):
        """
        Update an existing CloudFormation stack using change sets for safety.

        :param cfn_client: The CloudFormation boto3 client
        :type cfn_client: boto3.client
        :param stack_id: The ID of the stack to update
        :type stack_id: str
        :param describe_stack_response: The response from describe_stacks
        :type describe_stack_response: dict
        """
        try:
            log.trace("Updating existing stack '{}'", self.params.stack_name)

            # Create a change set first for safer updates
            change_set_name = f"update-{util.get_current_timestamp()}"

            args = {
                "StackName": stack_id,
                "TemplateURL": self.params.template_url,
                "Capabilities": CAPABILITITES,
                "Parameters": aws.transform_stack_parameter_hash(self.params.stack_parameters or {}),
                "ChangeSetName": change_set_name,
            }
            if self.params.tags:
                args["Tags"] = aws.transform_tag_hash(self.params.tags)
            if self.params.stack_policy:
                args["StackPolicyBody"] = util.to_json(self.params.stack_policy)

            # Create change set
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
                        self.params.stack_name,
                    )

                    # Delete the empty change set
                    cfn_client.delete_change_set(StackName=stack_id, ChangeSetName=change_set_name)

                    self.set_state("StackOperation", "NO_UPDATE")
                    self.set_state("NoUpdatesRequired", True)
                    self.set_output("StackOperation", "NO_UPDATE")
                    self.set_complete("No changes required")
                    self.__save_stack_outputs(describe_stack_response)
                    return

                # Log the changes for visibility
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

                self.set_running(f"Updating existing stack '{self.params.stack_name}'")
                log.debug("Stack update initiated via change set for: {}", stack_id)

            except ClientError as cs_error:
                if (
                    "No updates" in cs_error.response["Error"]["Message"]
                    or "didn't contain changes" in cs_error.response["Error"]["Message"]
                ):
                    log.debug("No updates required for stack '{}'", self.params.stack_name)
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
                self.params.stack_name,
                error_code,
                error_message,
            )
            self.set_failed(f"Failed to update stack '{self.params.stack_name}': {error_message}")
        except Exception as e:
            log.error("Unexpected error updating stack '{}': {}", self.params.stack_name, e)
            self.set_failed(f"Unexpected error updating stack '{self.params.stack_name}': {e}")

        log.trace("Stack update initiated")

    def _check(self):
        """
        Check the status of the CloudFormation stack operation.

        This method monitors the stack creation/update progress and saves outputs
        when the operation completes successfully.

        :raises: Sets action to failed if stack operation fails
        """
        log.trace("Checking CreateStackAction")

        stack_id = self.get_state("StackId")
        if not stack_id:
            self.set_failed("No stack ID found in state")
            return

        # Obtain a CloudFormation client
        try:
            cfn_client = aws.cfn_client(
                region=self.params.region,
                role=util.get_provisioning_role_arn(self.params.account),
            )
        except Exception as e:
            log.error("Failed to create CloudFormation client for status check: {}", e)
            self.set_failed(f"Failed to create CloudFormation client: {e}")
            return

        # Describe the stack to get its status
        try:
            describe_stack_response = cfn_client.describe_stacks(StackName=stack_id)
            if not describe_stack_response.get("Stacks"):
                self.set_failed(f"Stack '{stack_id}' not found")
                return

            stack_info = describe_stack_response["Stacks"][0]
            stack_status = stack_info["StackStatus"]

            # Update state with comprehensive stack information
            self.set_state("StackStatus", stack_status)
            self.set_state("LastChecked", util.get_current_timestamp())
            self.set_state("StackStatusReason", stack_info.get("StackStatusReason", ""))
            self.set_output("StackStatus", stack_status)

            # Enhanced status classification
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
                # Get more detailed failure information
                failure_reason = stack_info.get("StackStatusReason", "Unknown failure")
                self.set_state("StackOperationFailed", True)
                self.set_state("FailureReason", failure_reason)

                # Try to get stack events for more details
                self._capture_stack_events(cfn_client, stack_id, failed=True)

                self.set_failed(f"Stack operation failed: {stack_status} - {failure_reason}")

            elif stack_status in rollback_states:
                # Stack rolled back - this is usually a failure scenario
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

                # Capture intermediate events for monitoring
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
        """
        Rollback the CloudFormation stack operation.

        This method deletes the created stack or reverts changes made during update.

        .. note::
            Stack deletion may take significant time depending on the resources.
        """
        log.trace("Unexecuting CreateStackAction")

        stack_id = self.get_state("StackId")
        if not stack_id:
            log.debug("No stack ID found in state - nothing to rollback")
            self.set_complete("No stack to rollback")
            return

        try:
            cfn_client = aws.cfn_client(
                region=self.params.region,
                role=util.get_provisioning_role_arn(self.params.account),
            )
        except Exception as e:
            log.error("Failed to create CloudFormation client for rollback: {}", e)
            self.set_failed(f"Failed to create CloudFormation client for rollback: {e}")
            return

        try:
            cfn_client.delete_stack(StackName=stack_id)
            log.debug("Initiated deletion of stack '{}'", stack_id)

            self.set_state("StackDeletionStarted", True)
            self.set_running(f"Deleting stack '{self.params.stack_name}'")

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
        """
        Cancel the CloudFormation stack operation.

        This method attempts to cancel the current stack operation if possible.

        .. note::
            Not all stack operations can be cancelled depending on their current state.
        """
        log.trace("Cancelling CreateStackAction")

        stack_id = self.get_state("StackId")
        if not stack_id:
            self.set_complete("No stack operation to cancel")
            return

        try:
            cfn_client = aws.cfn_client(
                region=self.params.region,
                role=util.get_provisioning_role_arn(self.params.account),
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
        """
        Capture and log stack events for troubleshooting.

        :param cfn_client: The CloudFormation boto3 client
        :type cfn_client: boto3.client
        :param stack_id: The stack ID to get events for
        :type stack_id: str
        :param failed: Whether this is being called for a failed stack
        :type failed: bool
        """
        try:
            events_response = cfn_client.describe_stack_events(StackName=stack_id)
            events = events_response.get("StackEvents", [])

            # Store recent events count
            self.set_state("StackEventsCount", len(events))

            # If failed, capture the most recent failed events
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
        """
        Save CloudFormation stack outputs and resource summary to action outputs.

        :param describe_stack_response: The response from describe_stacks
        :type describe_stack_response: dict
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

            # Try to get resource summary
            self._capture_resource_summary()

            log.debug("Saved {} stack outputs and metadata", output_count)

        except (KeyError, IndexError, TypeError) as e:
            log.warning("Error saving stack outputs: {}", e)

    def _capture_resource_summary(self):
        """
        Capture a summary of stack resources for monitoring.
        """
        try:
            cfn_client = aws.cfn_client(
                region=self.params.region,
                role=util.get_provisioning_role_arn(self.params.account),
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
        """
        Check for stack drift after successful deployment.

        :param cfn_client: The CloudFormation boto3 client
        :type cfn_client: boto3.client
        :param stack_id: The stack ID to check for drift
        :type stack_id: str
        """
        try:
            # Initiate drift detection
            drift_response = cfn_client.detect_stack_drift(StackName=stack_id)
            drift_detection_id = drift_response.get("StackDriftDetectionId")

            if drift_detection_id:
                self.set_state("DriftDetectionId", drift_detection_id)
                log.debug("Initiated stack drift detection: {}", drift_detection_id)

                # Note: Drift detection is async, we don't wait for it
                # but store the ID for potential future reference

        except Exception as e:
            log.warning("Failed to initiate drift detection for stack '{}': {}", stack_id, e)

    @classmethod
    def generate_action_spec(cls, **kwargs) -> CreateStackActionSpec:
        return CreateStackActionSpec(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> CreateStackActionParams:
        return CreateStackActionParams(**kwargs)
