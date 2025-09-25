"""Find CloudFormation stacks that reference an export from a target stack.

Builds the export name as "<stack_name>:<output_name>" and uses
CloudFormation ListImports to discover importing stacks.
"""

from typing import Any
from pydantic import Field, model_validator
from botocore.exceptions import ClientError

import core_logging as log

import core_framework as util
import core_helper.aws as aws
from core_framework.models import DeploymentDetails, ActionResource, ActionSpec

from core_execute.actionlib.action import BaseAction


class GetStackReferencesActionSpec(ActionSpec):
    """Parameters for the GetStackReferences action.

    Attributes:
      account: AWS account ID where the stack resides.
      region: AWS region of the stack.
      stack_name: Name of the CloudFormation stack to inspect.
      output_name: Output export name to check (default: "DefaultExport").
    """

    stack_name: str = Field(
        ...,
        alias="StackName",
        description="The name of the stack to check for references (required)",
    )
    output_name: str = Field(
        default="DefaultExport",
        alias="OutputName",
        description="The output export name to check (default: 'DefaultExport')",
    )


class GetStackReferencesActionResource(ActionResource):
    """Resource model for GetStackReferences.

    Normalizes inputs and forces kind to 'AWS::GetStackReferences'.
    """

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Normalize incoming values and enforce canonical kind/spec."""
        if not isinstance(values, dict):
            return values

        values.pop("kind", None)
        values.pop("Kind", None)
        values["kind"] = "AWS::GetStackReferences"

        spec = values.pop("spec", None) or values.pop("Spec", None)
        if isinstance(spec, dict):
            values["spec"] = spec
        elif isinstance(spec, GetStackReferencesActionSpec):
            values["spec"] = spec.model_dump()

        return values


class GetStackReferencesAction(BaseAction[GetStackReferencesActionSpec]):
    """List stacks that import a specific CloudFormation export.

    Constructs "<stack_name>:<output_name>" and calls ListImports. Results and
    summary fields are stored in action state and outputs.
    """

    def __init__(
        self,
        definition: ActionResource,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        """Initialize the action and validate parameters.

        Args:
          definition: Action resource with metadata/spec.
          context: Rendering context for templates.
          deployment_details: Deployment metadata.

        """
        super().__init__(definition, context, deployment_details)

        # Validate the action parameters
        self.spec = GetStackReferencesActionSpec(**definition.spec)

    def _execute(self):
        """Call CloudFormation ListImports and record referencing stacks.

        Notes:
          - Missing export is treated as "no references" (success).
          - Existing export with zero imports is also success.
        """
        log.trace("GetStackReferencesAction._execute()")

        # Initialize state tracking
        start_time = util.get_current_timestamp()
        self.set_state("start_time", start_time)
        self.set_state("stack_name", self.spec.stack_name)
        self.set_state("output_name", self.spec.output_name)
        self.set_state("account", self.spec.account)
        self.set_state("region", self.spec.region)

        output_export_name = f"{self.spec.stack_name}:{self.spec.output_name}"
        self.set_state("export_name", output_export_name)

        self.set_running(f"Checking references for export '{output_export_name}'")

        # Obtain a CloudFormation client
        cfn_client = aws.cfn_client(
            region=self.spec.region,
            role_arn=util.get_provisioning_role_arn(self.spec.account),
        )

        try:
            response = cfn_client.list_imports(ExportName=output_export_name)

            # No error thrown - export exists; may or may not be referenced
            completion_time = util.get_current_timestamp()
            imports = response.get("Imports", [])
            num_references = len(imports)

            # Save comprehensive state
            self.set_state("completion_time", completion_time)
            self.set_state("status", "completed_with_references")
            self.set_state("num_references", num_references)
            self.set_state("references", imports)

            # Set comprehensive outputs
            self.set_output("stack_name", self.spec.stack_name)
            self.set_output("output_name", self.spec.output_name)
            self.set_output("export_name", output_export_name)
            self.set_output("account", self.spec.account)
            self.set_output("region", self.spec.region)
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
                    "StackName": self.spec.stack_name,
                    "OutputName": self.spec.output_name,
                    "ExportName": output_export_name,
                    "References": imports,
                    "HasReferences": True,
                    "NumReferences": num_references,
                },
            )

            # Complete the action
            self.set_complete(f"Export '{output_export_name}' is referenced by {num_references} stack(s)")

        except ClientError as e:
            completion_time = util.get_current_timestamp()
            error_message = e.response["Error"]["Message"]

            if "does not exist" in error_message:
                # Export doesn't exist - treat as unreferenced
                self.set_state("completion_time", completion_time)
                self.set_state("status", "completed_export_not_found")
                self.set_state("num_references", 0)
                self.set_state("references", [])

                # Set outputs for non-existent export
                self.set_output("stack_name", self.spec.stack_name)
                self.set_output("output_name", self.spec.output_name)
                self.set_output("export_name", output_export_name)
                self.set_output("account", self.spec.account)
                self.set_output("region", self.spec.region)
                self.set_output("references", [])
                self.set_output("has_references", False)
                self.set_output("num_references", 0)
                self.set_output("start_time", start_time)
                self.set_output("completion_time", completion_time)
                self.set_output("status", "success")
                self.set_output(
                    "message",
                    f"Export '{output_export_name}' does not exist; treating as no references",
                )

                self.set_complete(f"Export '{output_export_name}' does not exist; treating as unreferenced")

            elif "not imported" in error_message:
                # Export exists but isn't imported by any stacks
                self.set_state("completion_time", completion_time)
                self.set_state("status", "completed_no_references")
                self.set_state("num_references", 0)
                self.set_state("references", [])

                # Set outputs for unreferenced export
                self.set_output("stack_name", self.spec.stack_name)
                self.set_output("output_name", self.spec.output_name)
                self.set_output("export_name", output_export_name)
                self.set_output("account", self.spec.account)
                self.set_output("region", self.spec.region)
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
                        "StackName": self.spec.stack_name,
                        "OutputName": self.spec.output_name,
                        "ExportName": output_export_name,
                    },
                )
                self.set_complete(f"Export '{output_export_name}' is not referenced by any stacks")

            else:
                # Other error - set error state and re-raise
                self.set_state("error_time", completion_time)
                self.set_state("status", "error")
                self.set_state("error_message", error_message)

                # Set error outputs
                self.set_output("stack_name", self.spec.stack_name)
                self.set_output("output_name", self.spec.output_name)
                self.set_output("export_name", output_export_name)
                self.set_output("account", self.spec.account)
                self.set_output("region", self.spec.region)
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
                    self.spec.stack_name,
                    e,
                )
                raise

        log.trace("GetStackReferencesAction._execute() complete")

    def _check(self):
        """Not applicable; operation completes immediately."""
        log.trace("GetStackReferencesAction._check()")

        self.set_failed("Internal error - _check() should not have been called")

        log.trace("GetStackReferencesAction._check() complete")

    def _unexecute(self):
        """No rollback; this action is read-only."""
        pass

    def _cancel(self):
        """No-op; action completes immediately and cannot be cancelled."""
        pass

    def _resolve(self):
        """Render templates in account, region, stack_name, and output_name."""
        log.trace("GetStackReferencesAction._resolve()")

        self.spec.account = self.renderer.render_string(self.spec.account, self.context)
        self.spec.region = self.renderer.render_string(self.spec.region, self.context)
        self.spec.stack_name = self.renderer.render_string(self.spec.stack_name, self.context)
        self.spec.output_name = self.renderer.render_string(self.spec.output_name, self.context)

        log.trace("GetStackReferencesAction._resolve() complete")

    @classmethod
    def generate_action_resource(cls, **kwargs) -> GetStackReferencesActionResource:
        """Factory: create a typed GetStackReferencesActionResource."""
        return GetStackReferencesActionResource(**kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> GetStackReferencesActionSpec:
        """Factory: create typed GetStackReferencesActionSpec."""
        return GetStackReferencesActionSpec(**kwargs)
