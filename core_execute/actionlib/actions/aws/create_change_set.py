from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator
from botocore.exceptions import ClientError

import core_logging as log
import core_framework as util

from core_framework.models import ActionSpec, DeploymentDetails
from core_execute.actionlib.action import BaseAction
import core_helper.aws as aws


class CreateChangeSetActionParams(BaseModel):
    """Parameters for the CreateChangeSetAction

    This class defines the parameters that can be used in the action.
    """

    model_config = ConfigDict(populate_by_name=True, validate_assignment=True)

    account: str = Field(
        ..., alias="Account", description="The account where the action is located"
    )
    region: str = Field(
        ..., alias="Region", description="The region where the action is located"
    )
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
        ..., alias="ChangeSetName", description="The name of the change set to create"
    )
    stack_parameters: dict = Field(
        default_factory=dict,
        alias="StackParameters",
        description="Parameters for the CloudFormation template",
    )
    tags: dict = Field(
        default_factory=dict,
        alias="Tags",
        description="Tags to apply to the change set",
    )


class CreateChangeSetActionSpec(ActionSpec):
    """Generate the action definition"""

    @model_validator(mode="before")
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Validate the parameters for the CreateChangeSetActionSpec"""
        if not (values.get("name") or values.get("Name")):
            values["name"] = "action-aws-createchangeset-name"
        if not (values.get("kind") or values.get("Kind")):
            values["kind"] = "AWS::CreateChangeSet"
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
                "change_set_name": "",
            }
        return values


class CreateChangeSetAction(BaseAction):
    """CloudFormation Change Set Creation Action

    This action creates a CloudFormation change set for an existing stack.
    It supports cross-account deployments via role assumption and handles
    step function execution patterns with state persistence.

    Kind: Use the value: ``AWS::CreateChangeSet``

    .. rubric: ActionSpec:

    .. tip:: s3:/<bucket>/artifacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Name: action-aws-createchangeset-name
              Kind: "AWS::CreateChangeSet"
              Params:
                Account: "154798051514"
                Region: "ap-southeast-1"
                StackName: "my-stack"
                TemplateUrl: "s3://<bucket>/portfolio/template.yaml"
                ChangeSetName: "my-changeset"
                StackParameters:
                  Environment: "production"
                  InstanceType: "t3.micro"
                Tags:
                  Environment: "production"
                  Owner: "DevOps"
              Scope: "portfolio"

    State Variables:
        - ChangeSetCreationStarted: Timestamp when creation began
        - ChangeSetName: Name of the created change set
        - ChangeSetArn: ARN of the created change set
        - ChangeSetStatus: Current status of the change set
        - ChangeSetId: ID of the change set
        - StackId: ID of the target stack

    Output Variables:
        - ChangeSetArn: ARN of the successfully created change set
        - ChangeSetId: ID of the change set
        - StackId: ID of the target stack
        - Changes: List of changes in the change set
        - ChangesCount: Number of changes in the change set
    """

    def __init__(
        self,
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        self.params = CreateChangeSetActionParams(**definition.params)

    def _resolve(self):
        """
        Resolve template variables in action parameters.

        This method renders Jinja2 templates in the action parameters using the current context.
        """
        log.trace("Resolving CreateChangeSetAction")

        self.params.account = self.renderer.render_string(
            self.params.account, self.context
        )
        self.params.region = self.renderer.render_string(
            self.params.region, self.context
        )
        self.params.stack_name = self.renderer.render_string(
            self.params.stack_name, self.context
        )
        self.params.template_url = self.renderer.render_string(
            self.params.template_url, self.context
        )
        self.params.change_set_name = self.renderer.render_string(
            self.params.change_set_name, self.context
        )

        # Render stack parameters
        if self.params.stack_parameters:
            rendered_params = {}
            for key, value in self.params.stack_parameters.items():
                if isinstance(value, str):
                    rendered_params[key] = self.renderer.render_string(
                        value, self.context
                    )
                else:
                    rendered_params[key] = value
            self.params.stack_parameters = rendered_params

        # Render tags
        if self.params.tags:
            rendered_tags = {}
            for key, value in self.params.tags.items():
                if isinstance(value, str):
                    rendered_tags[key] = self.renderer.render_string(
                        value, self.context
                    )
                else:
                    rendered_tags[key] = value
            self.params.tags = rendered_tags

        log.trace("CreateChangeSetAction resolved")

    def _execute(self):
        """
        Execute the CloudFormation change set creation operation.

        This method creates the specified CloudFormation change set and sets appropriate
        state outputs for tracking.

        :raises: Sets action to failed if parameters are missing or CloudFormation operation fails
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
        if self.get_state("ChangeSetCreationStarted") and self.get_state(
            "ChangeSetArn"
        ):
            log.info(
                "Change set creation already in progress for {}",
                self.params.change_set_name,
            )
            self.set_running(
                f"Change set creation already in progress for {self.params.change_set_name}"
            )
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
            describe_response = cfn_client.describe_stacks(
                StackName=self.params.stack_name
            )

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
                self.set_failed(
                    f"Failed to check stack '{self.params.stack_name}': {e.response['Error']['Message']}"
                )
                return
        except Exception as e:
            log.error(
                "Unexpected error checking stack '{}': {}", self.params.stack_name, e
            )
            self.set_failed(
                f"Unexpected error checking stack '{self.params.stack_name}': {e}"
            )
            return

        self.set_state("StackExists", stack_exists)

        # Determine change set type
        change_set_type = "UPDATE" if stack_exists else "CREATE"
        self.set_state("ChangeSetType", change_set_type)

        # Attempt to create the change set
        self.set_running(
            f"Creating change set '{self.params.change_set_name}' for stack '{self.params.stack_name}'"
        )

        try:
            # Prepare change set parameters
            change_set_params = {
                "StackName": self.params.stack_name,
                "ChangeSetName": self.params.change_set_name,
                "TemplateURL": self.params.template_url,
                "ChangeSetType": change_set_type,
                "Capabilities": [  # Assumed capabilities as specified
                    "CAPABILITY_IAM",
                    "CAPABILITY_NAMED_IAM",
                    "CAPABILITY_AUTO_EXPAND",
                ],
            }

            # Add stack parameters if provided
            if self.params.stack_parameters:
                change_set_params["Parameters"] = [
                    {"ParameterKey": key, "ParameterValue": str(value)}
                    for key, value in self.params.stack_parameters.items()
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
            self.set_failed(
                f"Failed to create change set '{self.params.change_set_name}': {error_message}"
            )

        except Exception as e:
            log.error(
                "Unexpected error creating change set '{}': {}",
                self.params.change_set_name,
                e,
            )
            self.set_state("CreationResult", "FAILED")
            self.set_state("FailureReason", str(e))
            self.set_failed(
                f"Unexpected error creating change set '{self.params.change_set_name}': {e}"
            )

        log.trace("CreateChangeSetAction execution completed")

    def _check(self):
        """
        Check the status of the change set creation operation.

        This method monitors the CloudFormation change set creation progress and sets
        appropriate state based on the current status.
        """
        log.trace("Checking CreateChangeSetAction")

        change_set_arn = self.get_state("ChangeSetArn")

        if not change_set_arn:
            log.error("Change set ARN not found in state - execute may not have run")
            self.set_failed(
                "Change set ARN not found in state - execute may not have run"
            )
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

                self.set_complete(
                    f"Change set {self.params.change_set_name} created successfully with {len(changes)} changes"
                )
                log.info(
                    "Change set {} created successfully with {} changes",
                    self.params.change_set_name,
                    len(changes),
                )

            elif change_set_status in ["CREATE_IN_PROGRESS", "CREATE_PENDING"]:
                # Still creating
                self.set_running(
                    f"Change set {self.params.change_set_name} creation in progress"
                )

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
                self.set_running(
                    f"Change set {self.params.change_set_name} in unknown status: {change_set_status}"
                )

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
        """
        Rollback the change set creation operation.

        This method deletes the created change set to reverse the creation operation.
        """
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

            self.set_complete(
                f"Change set {self.params.change_set_name} deleted successfully"
            )
            log.info("Change set {} deleted successfully", self.params.change_set_name)

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            error_message = e.response["Error"]["Message"]

            if error_code == "ChangeSetNotFoundException":
                # Change set already deleted
                log.info(
                    "Change set {} was already deleted", self.params.change_set_name
                )
                self.set_state("RollbackResult", "ALREADY_DELETED")
                self.set_complete(
                    f"Change set {self.params.change_set_name} was already deleted"
                )
            else:
                log.error(
                    "Error deleting change set: {} - {}", error_code, error_message
                )
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
        """
        Cancel the change set creation operation.

        This method attempts to cancel an in-progress change set creation by deleting it.
        """
        log.trace("Cancelling CreateChangeSetAction")

        change_set_status = self.get_state("ChangeSetStatus")

        if change_set_status in ["CREATE_IN_PROGRESS", "CREATE_PENDING"]:
            # Try to delete the in-progress change set
            log.info("Cancelling in-progress change set creation")
            self._unexecute()
        else:
            log.info("Change set not in cancellable state: {}", change_set_status)
            self.set_complete(
                f"Change set not in cancellable state: {change_set_status}"
            )

        log.trace("CreateChangeSetAction cancellation completed")
