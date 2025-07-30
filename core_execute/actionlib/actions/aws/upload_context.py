"""Upload the Jinja2 Render context to the appropriate S3 bucket"""

from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator, field_validator
import re
import json

import core_logging as log

import core_helper.aws as aws

import core_framework as util
from core_helper.magic import MagicS3Client
from core_framework.models import ActionSpec, DeploymentDetails

from core_execute.actionlib.action import BaseAction


class UploadContextActionParams(BaseModel):
    """Parameters for the UploadContextAction.

    Contains all configuration needed to upload deployment context data
    to an S3 bucket in both YAML and JSON formats.

    Attributes
    ----------
    account : str
        The AWS account ID where the S3 bucket is located
    bucket_name : str
        The name of the S3 bucket to upload context files to
    region : str
        The AWS region where the S3 bucket is located
    prefix : str
        The S3 key prefix for organizing uploaded context files

    Examples
    --------
    Basic context upload configuration::

        params = UploadContextActionParams(
            account="123456789012",
            bucket_name="my-deployment-artifacts",
            region="us-east-1",
            prefix="deployments/myapp/v1.0.0"
        )
    """

    model_config = ConfigDict(populate_by_name=True, validate_assignment=True)

    account: str = Field(..., alias="Account", description="The AWS account ID where the S3 bucket is located")
    bucket_name: str = Field(
        ...,
        alias="BucketName",
        description="The name of the S3 bucket to upload context files to",
    )
    region: str = Field(..., alias="Region", description="The AWS region where the S3 bucket is located")
    prefix: str = Field(
        ...,
        alias="Prefix",
        description="The S3 key prefix for organizing uploaded context files",
    )

    @field_validator("account")
    @classmethod
    def validate_account(cls, v: str) -> str:
        """Validate that account is a valid AWS account ID.

        Parameters
        ----------
        v : str
            The account ID to validate

        Returns
        -------
        str
            The validated account ID

        Raises
        ------
        ValueError
            If account ID format is invalid
        """
        if not v:
            raise ValueError("Account cannot be empty")
        if not v.isdigit():
            raise ValueError("Account must be a numeric AWS account ID")
        if len(v) != 12:
            raise ValueError("Account ID must be exactly 12 digits")
        return v

    @field_validator("bucket_name")
    @classmethod
    def validate_bucket_name(cls, v: str) -> str:
        """Validate that bucket_name follows S3 naming conventions.

        Parameters
        ----------
        v : str
            The bucket name to validate

        Returns
        -------
        str
            The validated bucket name

        Raises
        ------
        ValueError
            If bucket name format is invalid
        """
        if not v:
            raise ValueError("BucketName cannot be empty")
        if len(v) < 3 or len(v) > 63:
            raise ValueError("Bucket name must be between 3 and 63 characters")
        if not re.match(r"^[a-z0-9.-]+$", v):
            raise ValueError("Bucket name can only contain lowercase letters, numbers, hyphens, and periods")
        return v

    @field_validator("region")
    @classmethod
    def validate_region(cls, v: str) -> str:
        """Validate that region is a valid AWS region format.

        Parameters
        ----------
        v : str
            The region to validate

        Returns
        -------
        str
            The validated region

        Raises
        ------
        ValueError
            If region format is invalid
        """
        if not v:
            raise ValueError("Region cannot be empty")
        if not re.match(r"^[a-z0-9-]+$", v):
            raise ValueError("Region must contain only lowercase letters, numbers, and hyphens")
        return v

    @field_validator("prefix")
    @classmethod
    def validate_prefix(cls, v: str) -> str:
        """Validate that prefix follows S3 key naming conventions.

        Parameters
        ----------
        v : str
            The prefix to validate

        Returns
        -------
        str
            The validated prefix

        Raises
        ------
        ValueError
            If prefix format is invalid
        """
        if not v:
            raise ValueError("Prefix cannot be empty")
        # Remove leading/trailing slashes for consistency
        v = v.strip("/")
        if not v:
            raise ValueError("Prefix cannot be just slashes")
        return v


class UploadContextActionSpec(ActionSpec):
    """Generate the action definition for UploadContext.

    Provides a convenience wrapper for creating UploadContext actions
    with sensible defaults for common context upload use cases.

    Examples
    --------
    Creating a context upload action spec with defaults::

        spec = UploadContextActionSpec()
        # Results in action with name "upload-context", kind "upload_context"
        # and template-based default parameters
    """

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Validate and set default parameters for the UploadContextActionSpec.

        Provides sensible defaults for action name, kind, scope, and
        basic parameter structure using template variables.

        Parameters
        ----------
        values : dict[str, Any]
            The input values dictionary

        Returns
        -------
        dict[str, Any]
            The values dictionary with defaults applied
        """
        if not (values.get("name") or values.get("Name")):
            values["name"] = "upload-context"
        if not (values.get("kind") or values.get("Kind")):
            values["kind"] = "AWS::UploadContext"
        if not (values.get("depends_on") or values.get("DependsOn")):
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Params")):
            values["params"] = {
                "Account": "{{ deployment.account }}",
                "BucketName": "{{ artifacts.bucket_name }}",
                "Region": "{{ deployment.region }}",
                "Prefix": "{{ deployment.portfolio }}/{{ app.name }}/{{ branch.name }}/{{ build.version }}",
            }
        return values


class UploadContextAction(BaseAction):
    """Upload deployment context data to S3 in YAML and JSON formats.

    This action extracts output variables from the deployment context and uploads
    them to S3 as both YAML and JSON files. The context includes all output values
    from previous actions, providing a complete deployment state snapshot.

    **Key Features:**

    - Extract output variables from deployment context
    - Upload context in both YAML and JSON formats
    - Organize outputs by component and pipeline structure
    - Support for template variables in S3 paths
    - Server-side encryption for uploaded files

    **Use Cases:**

    - Create deployment state snapshots for audit purposes
    - Share deployment outputs with external systems
    - Enable post-deployment analysis and debugging
    - Provide input data for downstream processes

    **Action Parameters:**

    :param Account: AWS account ID where the S3 bucket is located
    :type Account: str
    :param BucketName: Name of the S3 bucket to upload context files to
    :type BucketName: str
    :param Region: AWS region where the S3 bucket is located
    :type Region: str
    :param Prefix: S3 key prefix for organizing uploaded context files
    :type Prefix: str

    **Examples:**

    Simple context upload to artifacts bucket:

    .. code-block:: yaml

        - name: upload-deployment-context
          kind: upload_context
          params:
            Account: "{{ deployment.account }}"
            BucketName: "{{ artifacts.bucket_name }}"
            Region: "{{ deployment.region }}"
            Prefix: "deployments/{{ app.name }}/{{ branch.name }}/{{ build.version }}"

    Custom context upload with specific organization:

    .. code-block:: yaml

        - name: upload-context-snapshot
          kind: upload_context
          params:
            Account: "123456789012"
            BucketName: "deployment-snapshots"
            Region: "us-east-1"
            Prefix: "{{ deployment.portfolio }}/{{ deployment.environment }}/{{ deployment.timestamp }}"

    **Context Structure:**

    The uploaded context includes:

    - **Component outputs**: Organized by component name
    - **Pipeline outputs**: Application-level outputs
    - **Nested structure**: Hierarchical organization of output data
    - **Multiple formats**: Both YAML and JSON for flexibility

    **Security Considerations:**

    - Files are encrypted at rest using AES256 server-side encryption
    - Requires appropriate S3 write permissions to target bucket
    - Context may contain sensitive deployment information
    - Access should be restricted based on deployment security requirements

    **State Tracking:**

    This action tracks execution state:

    - ``uploaded_files`` - List of S3 keys for uploaded files
    - ``yaml_file`` - S3 key for the YAML context file
    - ``json_file`` - S3 key for the JSON context file
    - ``file_count`` - Number of context variables uploaded
    - ``status`` - Success/error status of the upload operation
    """

    def __init__(
        self,
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        """Initialize the UploadContextAction.

        Parameters
        ----------
        definition : ActionSpec
            The action specification containing parameters and configuration
        context : dict[str, Any]
            Template rendering context with deployment variables and outputs
        deployment_details : DeploymentDetails
            Deployment context and metadata
        """
        super().__init__(definition, context, deployment_details)

        # Validate the action parameters
        self.params = UploadContextActionParams(**definition.params)

    def __context_outputs(self) -> dict[str, Any]:
        """Extract and organize output variables from the deployment context.

        Processes the deployment context to extract output variables and organize
        them by component and pipeline structure. Only processes values with
        PRNs ending in ":output" to avoid including internal state data.

        Returns
        -------
        dict[str, Any]
            Dictionary of organized output variables with hierarchical structure

        Notes
        -----
        This method parses PRNs (Portable Resource Names) to determine the
        source and organization of output variables. Component outputs are
        organized under their component name, while app-level outputs are
        organized under "pipeline".
        """
        log.trace("UploadContextAction.__context_outputs()")

        outputs = {}

        for key, value in self.context.items():
            # Skip non-output values
            if "/" not in key:
                continue

            prn, name = key.split("/", 1)

            # Only upload output values (not vars or action state)
            if not prn.endswith(":output"):
                continue

            try:
                # Example PRN is prn:demo:ecs:testing:2:web2:action/deploy/main/upload-context
                if prn.count(":") == 6:
                    # Component PRN
                    _, portfolio, app, branch, build, component, resource_type = prn.split(":")
                    var_name = "{}/{}".format(component, name)
                elif prn.count(":") == 5:
                    # App PRN
                    _, portfolio, app, branch, build, resource_type = prn.split(":")
                    var_name = "{}/{}".format("pipeline", name)
                else:
                    log.warning(f"Unsupported PRN format: {prn}")
                    continue

                outputs[var_name] = value

            except Exception as e:
                log.warning(f"Failed to process context key {key}: {str(e)}")
                continue

        log.debug(f"Extracted {len(outputs)} context output variables")
        return outputs

    def _execute(self):
        """Execute the context upload operation.

        Extracts output variables from the deployment context, organizes them
        into a hierarchical structure, and uploads the data to S3 in both
        YAML and JSON formats with server-side encryption.

        The execution process:

        1. Extracts output variables from deployment context
        2. Organizes variables into nested dictionary structure
        3. Creates S3 client for target region
        4. Uploads context data as YAML file
        5. Uploads context data as JSON file
        6. Records uploaded file information in action state

        Raises
        ------
        Exception
            If S3 upload fails or context processing errors occur

        Notes
        -----
        This method implements the core functionality and should not be
        called directly. Use the action execution framework instead.
        """
        log.trace("UploadContextAction._execute()")

        try:
            # Extract and organize context outputs
            context_outputs = self.__context_outputs()

            if not context_outputs:
                log.warning("No context outputs found to upload")
                self.set_state("status", "skipped")
                self.set_state("variable_count", 0)
                self.set_complete("No context outputs to upload")
                return

            # Build nested structure for output
            body_hash = {}
            for key, value in context_outputs.items():
                var_path = key.split("/")
                util.set_nested(body_hash, var_path, value)

            # Create S3 client
            role_arn = util.get_provisioning_role_arn(self.params.account)
            client = MagicS3Client.get_client(self.params.region, role_arn)

            uploaded_files = []

            # Upload context as YAML
            yaml_key = "{}/context.yaml".format(self.params.prefix)
            log.debug(f"Uploading YAML context file '{yaml_key}' to '{self.params.bucket_name}'")

            yaml_result = client.put_object(
                Bucket=self.params.bucket_name,
                Key=yaml_key,
                Body=util.to_yaml(body_hash),
                ServerSideEncryption="AES256",
            )
            uploaded_files.append(yaml_key)

            # Upload context as JSON
            json_key = "{}/context.json".format(self.params.prefix)
            log.debug(f"Uploading JSON context file '{json_key}' to '{self.params.bucket_name}'")

            json_result = client.put_object(
                Bucket=self.params.bucket_name,
                Key=json_key,
                Body=util.to_json(body_hash),
                ServerSideEncryption="AES256",
            )
            uploaded_files.append(json_key)

            # Record successful upload
            self.set_state("status", "success")
            self.set_state("uploaded_files", uploaded_files)
            self.set_state("yaml_file", yaml_key)
            self.set_state("json_file", json_key)
            self.set_state("variable_count", len(context_outputs))
            self.set_state("bucket_name", self.params.bucket_name)
            self.set_state("prefix", self.params.prefix)

            success_message = f"Successfully uploaded context files: {uploaded_files}"
            log.info(success_message)
            self.set_complete(success_message)

        except Exception as e:
            error_message = f"Failed to upload context files: {str(e)}"
            log.error(error_message)
            self.set_state("status", "error")
            self.set_state("error_message", error_message)
            self.set_failed(error_message)

        log.trace("UploadContextAction._execute() complete")

    def _check(self):
        """Check operation - verify uploaded files exist in S3.

        Verifies that the context files were successfully uploaded to S3
        by checking their existence and retrieving metadata.

        Raises
        ------
        Exception
            If file verification fails or files are missing

        Notes
        -----
        This method verifies the operation result and should not be
        called directly. Use the action execution framework instead.
        """
        log.trace("UploadContextAction._check()")

        try:
            uploaded_files = self.get_state("uploaded_files")
            if not uploaded_files:
                error_message = "No uploaded files found in state to verify"
                log.error(error_message)
                self.set_failed(error_message)
                return

            # Create S3 client
            client = MagicS3Client(Region=self.params.region)

            # Verify each uploaded file exists
            for file_key in uploaded_files:
                try:
                    response = client.head_object(Bucket=self.params.bucket_name, Key=file_key)
                    log.debug(f"Verified file exists: s3://{self.params.bucket_name}/{file_key}")
                except Exception as e:
                    error_message = f"Failed to verify file s3://{self.params.bucket_name}/{file_key}: {str(e)}"
                    log.error(error_message)
                    self.set_failed(error_message)
                    return

            success_message = f"Verified all {len(uploaded_files)} context files exist in S3"
            log.info(success_message)
            self.set_complete(success_message)

        except Exception as e:
            error_message = f"Failed to check uploaded context files: {str(e)}"
            log.error(error_message)
            self.set_failed(error_message)

        log.trace("UploadContextAction._check() complete")

    def _unexecute(self):
        """Unexecute operation - remove uploaded context files.

        Removes the context files that were uploaded during execution,
        effectively cleaning up the S3 objects created by this action.

        Notes
        -----
        This operation attempts to delete previously uploaded files.
        If files are already deleted or don't exist, the operation succeeds.
        """
        log.trace("UploadContextAction._unexecute()")

        try:
            uploaded_files = self.get_state("uploaded_files")
            if not uploaded_files:
                log.debug("No uploaded files found in state - nothing to unexecute")
                return

            # Create S3 client
            client = MagicS3Client(Region=self.params.region)

            # Delete each uploaded file
            for file_key in uploaded_files:
                try:
                    client.delete_object(Bucket=self.params.bucket_name, Key=file_key)
                    log.debug(f"Deleted file: s3://{self.params.bucket_name}/{file_key}")
                except Exception as e:
                    log.warning(f"Failed to delete file {file_key}: {str(e)}")

            log.info(f"Successfully removed {len(uploaded_files)} context files from S3")

        except Exception as e:
            log.warning(f"Failed to remove context files during unexecute: {str(e)}")
            # Don't fail the unexecute operation for cleanup issues

        log.trace("UploadContextAction._unexecute() complete")

    def _cancel(self):
        """Cancel operation - not applicable for context upload.

        Context upload operations complete quickly and cannot be meaningfully cancelled.
        This is a no-op method as the upload operation is atomic.

        Notes
        -----
        This is a no-op method as S3 upload operations cannot be cancelled.
        """
        log.debug("Cancel requested for context upload - operation cannot be cancelled")

    def _resolve(self):
        """Resolve template variables and prepare parameters for execution.

        Renders all template variables in the action parameters using the
        provided context. This includes account ID, bucket name, region, and prefix.

        **Template Variables Available:**

        - ``deployment.*`` - Deployment context (account, region, environment)
        - ``app.*`` - Application information (name, version, config)
        - ``artifacts.*`` - Artifacts configuration (bucket_name, region)
        - ``branch.*`` - Branch details (name, type, commit)
        - ``build.*`` - Build information (version, number, timestamp)
        - ``env.*`` - Environment variables

        Raises
        ------
        Exception
            If template rendering fails or parameter validation errors occur

        Notes
        -----
        This method prepares data for execution and should not be
        called directly. Use the action execution framework instead.
        """
        log.trace("UploadContextAction._resolve()")

        try:
            # Render template variables
            self.params.account = self.renderer.render_string(self.params.account, self.context)
            self.params.bucket_name = self.renderer.render_string(self.params.bucket_name, self.context)
            self.params.region = self.renderer.render_string(self.params.region, self.context)
            self.params.prefix = self.renderer.render_string(self.params.prefix, self.context)

            # Clean up prefix (remove leading/trailing slashes)
            self.params.prefix = self.params.prefix.strip("/")

            log.debug(f"Resolved context upload to s3://{self.params.bucket_name}/{self.params.prefix}/")

        except Exception as e:
            error_message = f"Failed to resolve template variables: {str(e)}"
            log.error(error_message)
            self.set_failed(error_message)

        log.trace("UploadContextAction._resolve() complete")
