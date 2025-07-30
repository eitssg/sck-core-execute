"""Get the references to a CloudFormation stack output export action for Core Execute automation platform."""

from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator
from botocore.exceptions import ClientError

import core_logging as log

import core_framework as util
import core_helper.aws as aws
from core_framework.models import DeploymentDetails, ActionSpec

from core_execute.actionlib.action import BaseAction


class GetStackReferencesActionParams(BaseModel):
    """
    Parameters for the GetStackReferencesAction.

    Attributes
    ----------
    account : str
        The AWS account ID where the CloudFormation stack is located.
    region : str
        The AWS region where the CloudFormation stack is located.
    stack_name : str
        The name of the CloudFormation stack to check for references.
    output_name : str
        The name of the output export to check for references.
        Defaults to 'DefaultExport' if not specified.
    """

    model_config = ConfigDict(populate_by_name=True, validate_assignment=True)

    account: str = Field(
        ..., alias="Account", description="The account to use for the action (required)"
    )
    region: str = Field(
        ...,
        alias="Region",
        description="The region where the stack is located (required)",
    )
    stack_name: str = Field(
        ...,
        alias="StackName",
        description="The name of the stack to check for references (required)",
    )
    output_name: str = Field(
        default="DefaultExport",
        alias="OutputName",
        description="The name of the output to check for references (optional) defaults to 'DefaultExport'",
    )


class GetStackReferencesActionSpec(ActionSpec):
    """
    Action specification for the GetStackReferences action.

    Provides validation and default values for GetStackReferences action definitions.
    """

    @model_validator(mode="before")
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """
        Validate and set default parameters for the GetStackReferencesActionSpec.

        :param values: Input values dictionary.
        :type values: dict[str, Any]
        :return: Validated values with defaults applied.
        :rtype: dict[str, Any]
        """
        if not (values.get("name") or values.get("Name")):
            values["name"] = "action-aws-getstackreferences-name"
        if not (values.get("kind") or values.get("Kind")):
            values["kind"] = "AWS::GetStackReferences"
        if not (values.get("depends_on") or values.get("DependsOn")):
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Params")):
            values["params"] = {
                "account": "",
                "region": "",
                "stack_name": "",
                "output_name": "DefaultExport",
            }
        return values


class GetStackReferencesAction(BaseAction):
    """
    Get the references to a CloudFormation stack output export.

    This action checks which other CloudFormation stacks are importing/referencing
    a specific output export from the target stack. It uses the CloudFormation
    list_imports API to find all stacks that import the specified export.

    The export name is constructed as ``{stack_name}:{output_name}`` following
    CloudFormation export naming conventions.

    Attributes
    ----------
    params : GetStackReferencesActionParams
        Validated parameters for the action.

    Parameters
    ----------
    Kind : str
        Use the value: ``AWS::GetStackReferences``
    Params.Account : str
        The AWS account where the stack is located
    Params.Region : str
        The AWS region where the stack is located
    Params.StackName : str
        The name of the stack to check for references (required)
    Params.OutputName : str
        The name of the output to check for references (optional, defaults to 'DefaultExport')

    Examples
    --------
    ActionSpec YAML configuration:

    .. code-block:: yaml

        - Name: action-aws-getstackreferences-name
          Kind: "AWS::GetStackReferences"
          Params:
            Account: "154798051514"
            StackName: "my-stack-name"
            Region: "ap-southeast-1"
            OutputName: "DefaultExport"
          Scope: "build"

    Notes
    -----
    The action will complete successfully even if:

    - The export doesn't exist (treated as no references)
    - The export exists but isn't imported by any stacks
    - The stack itself doesn't exist

    Only genuine CloudFormation API errors will cause the action to fail.

    The references information is stored in the action outputs and can be used
    by subsequent actions to make decisions about stack deletion or updates.
    """

    def __init__(
        self,
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        """
        Initialize the GetStackReferencesAction.

        :param definition: The action specification definition.
        :type definition: ActionSpec
        :param context: Execution context for variable resolution.
        :type context: dict[str, Any]
        :param deployment_details: Details about the current deployment.
        :type deployment_details: DeploymentDetails
        :raises ValidationError: If action parameters are invalid.
        """
        super().__init__(definition, context, deployment_details)

        # Validate the action parameters
        self.params = GetStackReferencesActionParams(**definition.params)

    def _execute(self):
        """
        Execute the stack references check operation.

        Connects to CloudFormation using the provisioning role and checks
        which stacks are importing the specified output export. Stores
        comprehensive information about the references found.

        :raises ClientError: If CloudFormation operations fail (except for expected cases).
        """
        log.trace("GetStackReferencesAction._execute()")

        # Initialize state tracking
        start_time = util.get_current_timestamp()
        self.set_state("start_time", start_time)
        self.set_state("stack_name", self.params.stack_name)
        self.set_state("output_name", self.params.output_name)
        self.set_state("account", self.params.account)
        self.set_state("region", self.params.region)

        output_export_name = f"{self.params.stack_name}:{self.params.output_name}"
        self.set_state("export_name", output_export_name)

        self.set_running(f"Checking references for export '{output_export_name}'")

        # Obtain a CloudFormation client
        cfn_client = aws.cfn_client(
            region=self.params.region,
            role=util.get_provisioning_role_arn(self.params.account),
        )

        try:
            response = cfn_client.list_imports(ExportName=output_export_name)

            # No error thrown - stack is being referenced
            completion_time = util.get_current_timestamp()
            imports = response.get("Imports", [])
            num_references = len(imports)

            # Save comprehensive state
            self.set_state("completion_time", completion_time)
            self.set_state("status", "completed_with_references")
            self.set_state("num_references", num_references)
            self.set_state("references", imports)

            # Set comprehensive outputs
            self.set_output("stack_name", self.params.stack_name)
            self.set_output("output_name", self.params.output_name)
            self.set_output("export_name", output_export_name)
            self.set_output("account", self.params.account)
            self.set_output("region", self.params.region)
            self.set_output("references", imports)
            self.set_output("has_references", True)
            self.set_output("num_references", num_references)
            self.set_output("start_time", start_time)
            self.set_output("completion_time", completion_time)
            self.set_output("status", "success")
            self.set_output(
                "message",
                f"Export '{output_export_name}' is referenced by {num_references} stack(s)",
            )

            log.debug(
                "Stack export is being referenced",
                details={
                    "StackName": self.params.stack_name,
                    "OutputName": self.params.output_name,
                    "ExportName": output_export_name,
                    "References": imports,
                    "HasReferences": True,
                    "NumReferences": num_references,
                },
            )

            # Complete the action
            self.set_complete(
                f"Export '{output_export_name}' is referenced by {num_references} stack(s)"
            )

        except ClientError as e:
            completion_time = util.get_current_timestamp()
            error_message = e.response["Error"]["Message"]

            if "does not exist" in error_message:
                # Export doesn't exist - treat as unreferenced stack
                self.set_state("completion_time", completion_time)
                self.set_state("status", "completed_export_not_found")
                self.set_state("num_references", 0)
                self.set_state("references", [])

                # Set outputs for non-existent export
                self.set_output("stack_name", self.params.stack_name)
                self.set_output("output_name", self.params.output_name)
                self.set_output("export_name", output_export_name)
                self.set_output("account", self.params.account)
                self.set_output("region", self.params.region)
                self.set_output("references", [])
                self.set_output("has_references", False)
                self.set_output("num_references", 0)
                self.set_output("start_time", start_time)
                self.set_output("completion_time", completion_time)
                self.set_output("status", "success")
                self.set_output(
                    "message",
                    f"Export '{output_export_name}' does not exist, treating as no references",
                )

                self.set_complete(
                    f"Export '{output_export_name}' does not exist, treating stack as unreferenced"
                )

            elif "not imported" in error_message:
                # Export exists but isn't imported by any stacks
                self.set_state("completion_time", completion_time)
                self.set_state("status", "completed_no_references")
                self.set_state("num_references", 0)
                self.set_state("references", [])

                # Set outputs for unreferenced export
                self.set_output("stack_name", self.params.stack_name)
                self.set_output("output_name", self.params.output_name)
                self.set_output("export_name", output_export_name)
                self.set_output("account", self.params.account)
                self.set_output("region", self.params.region)
                self.set_output("references", [])
                self.set_output("has_references", False)
                self.set_output("num_references", 0)
                self.set_output("start_time", start_time)
                self.set_output("completion_time", completion_time)
                self.set_output("status", "success")
                self.set_output(
                    "message",
                    f"Export '{output_export_name}' is not referenced by any stacks",
                )

                log.warning(
                    "Stack export is not referenced",
                    details={
                        "StackName": self.params.stack_name,
                        "OutputName": self.params.output_name,
                        "ExportName": output_export_name,
                    },
                )
                self.set_complete(
                    f"Export '{output_export_name}' is not referenced by any stacks"
                )

            else:
                # Other error - set error state
                self.set_state("error_time", completion_time)
                self.set_state("status", "error")
                self.set_state("error_message", error_message)

                # Set error outputs
                self.set_output("stack_name", self.params.stack_name)
                self.set_output("output_name", self.params.output_name)
                self.set_output("export_name", output_export_name)
                self.set_output("account", self.params.account)
                self.set_output("region", self.params.region)
                self.set_output("start_time", start_time)
                self.set_output("error_time", completion_time)
                self.set_output("status", "error")
                self.set_output("error_message", error_message)
                self.set_output(
                    "message",
                    f"Error checking references for export '{output_export_name}': {error_message}",
                )

                log.error(
                    "Error getting references for stack '{}': {}",
                    self.params.stack_name,
                    e,
                )
                raise

        log.trace("GetStackReferencesAction._execute() complete")

    def _check(self):
        """
        Check the status of the stack references operation.

        This method should not be called for GetStackReferences actions as the
        operation completes immediately. If called, it indicates an internal error.
        """
        log.trace("GetStackReferencesAction._check()")

        self.set_failed("Internal error - _check() should not have been called")

        log.trace("GetStackReferencesAction._check() complete")

    def _unexecute(self):
        """
        Reverse the stack references operation.

        This operation cannot be reversed as it only reads data.
        This method is provided for interface compliance but performs no action.
        """
        pass

    def _cancel(self):
        """
        Cancel the stack references operation.

        This operation cannot be cancelled as it completes immediately.
        This method is provided for interface compliance but performs no action.
        """
        pass

    def _resolve(self):
        """
        Resolve template variables in action parameters.

        Uses the renderer to substitute variables in the account, region,
        stack_name, and output_name parameters using the current execution context.
        """
        log.trace("GetStackReferencesAction._resolve()")

        self.params.account = self.renderer.render_string(
            self.params.account, self.context
        )
        self.params.region = self.renderer.render_string(
            self.params.region, self.context
        )
        self.params.stack_name = self.renderer.render_string(
            self.params.stack_name, self.context
        )
        self.params.output_name = self.renderer.render_string(
            self.params.output_name, self.context
        )

        log.trace("GetStackReferencesAction._resolve() complete")
