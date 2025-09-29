"""Upload the rendered deployment context to S3 as YAML and JSON.

Collects output variables from the action context and writes them to an S3 bucket
under a configurable prefix. Useful for snapshots, audit, and downstream use.
"""

from typing import Any
from pydantic import Field, model_validator

import core_logging as log

import core_framework as util
from core_helper.magic import MagicS3Client
from core_framework.models import ActionResource, DeploymentDetails, ActionSpec

from core_execute.actionlib.action import BaseAction


class UploadContextActionSpec(ActionSpec):
    """Parameters for the UploadContext action.

    Attributes:
      account: AWS account ID for role resolution.
      region: AWS region of the target bucket.
      bucket_name: Target S3 bucket name.
      prefix: S3 key prefix (folder) for uploaded files.
    """

    bucket_name: str = Field(
        ...,
        alias="BucketName",
        description="The name of the S3 bucket to upload context files to",
    )
    prefix: str = Field(
        ...,
        alias="Prefix",
        description="The S3 key prefix for organizing uploaded context files",
    )


class UploadContextActionResource(ActionResource):
    """Pydantic resource for UploadContext.

    Normalizes parameters and forces kind to 'AWS::UploadContext'.
    """

    @model_validator(mode="before")
    @classmethod
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Normalize incoming values and enforce canonical kind/spec.

        - Forces kind to 'AWS::UploadContext'
        - Accepts spec as dict or UploadContextActionSpec
        """
        if not isinstance(values, dict):
            return values

        values.pop("kind", None)
        values.pop("Kind", None)
        values["kind"] = "AWS::UploadContext"

        spec = values.pop("spec", None) or values.pop("Spec", None)
        if isinstance(spec, dict):
            values["spec"] = spec
        elif isinstance(spec, UploadContextActionSpec):
            values["spec"] = spec.model_dump()

        return values


class UploadContextAction(BaseAction[UploadContextActionSpec]):
    """Upload deployment context outputs to S3 (YAML and JSON).

    Extracts action output variables from the context, organizes them by
    component/pipeline, and uploads both YAML and JSON snapshots to S3 with SSE.
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
          context: Rendering context with deployment variables and outputs.
          deployment_details: Deployment metadata (portfolio/app/branch/build).

        """
        super().__init__(definition, context, deployment_details)

        self.spec = UploadContextActionSpec.model_validate(definition.spec)

    def __context_outputs(self) -> dict[str, Any]:
        """Collect output variables from the context.

        Returns:
          A dict keyed by "component/<name>" or "pipeline/<name>" for app-level
          values, including only keys whose PRN ends with ':output'.
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
                # Example PRN: prn:demo:ecs:testing:2:web2:action/...
                if prn.count(":") == 6:
                    # Component PRN
                    _, portfolio, app, branch, build, component, resource_type = prn.split(":")
                    var_name = f"{component}/{name}"
                elif prn.count(":") == 5:
                    # App-level PRN
                    _, portfolio, app, branch, build, resource_type = prn.split(":")
                    var_name = f"pipeline/{name}"
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
        """Upload the context snapshot to S3.

        Steps:
          1) Extract output variables
          2) Build nested structure
          3) Upload YAML and JSON with SSE
          4) Record state (files, counts, status)
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
            role_arn = util.get_provisioning_role_arn(self.spec.account)
            client = MagicS3Client.get_client(self.spec.region, role_arn)

            uploaded_files = []

            # Upload context as YAML
            yaml_key = f"{self.spec.prefix}/context.yaml"
            log.debug(f"Uploading YAML context file '{yaml_key}' to '{self.spec.bucket_name}'")

            client.put_object(
                Bucket=self.spec.bucket_name,
                Key=yaml_key,
                Body=util.to_yaml(body_hash),
                ServerSideEncryption="AES256",
            )
            uploaded_files.append(yaml_key)

            # Upload context as JSON
            json_key = f"{self.spec.prefix}/context.json"
            log.debug(f"Uploading JSON context file '{json_key}' to '{self.spec.bucket_name}'")

            client.put_object(
                Bucket=self.spec.bucket_name,
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
            self.set_state("bucket_name", self.spec.bucket_name)
            self.set_state("prefix", self.spec.prefix)

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
        """Verify uploaded files exist in S3."""
        log.trace("UploadContextAction._check()")

        try:
            uploaded_files = self.get_state("uploaded_files")
            if not uploaded_files:
                error_message = "No uploaded files found in state to verify"
                log.error(error_message)
                self.set_failed(error_message)
                return

            # Create S3 client
            client = MagicS3Client(Region=self.spec.region)

            # Verify each uploaded file exists
            for file_key in uploaded_files:
                try:
                    client.head_object(Bucket=self.spec.bucket_name, Key=file_key)
                    log.debug(f"Verified file exists: s3://{self.spec.bucket_name}/{file_key}")
                except Exception as e:
                    error_message = f"Failed to verify file s3://{self.spec.bucket_name}/{file_key}: {str(e)}"
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
        """Delete previously uploaded context files (best-effort)."""
        log.trace("UploadContextAction._unexecute()")

        try:
            uploaded_files = self.get_state("uploaded_files")
            if not uploaded_files:
                log.debug("No uploaded files found in state - nothing to unexecute")
                return

            # Create S3 client
            client = MagicS3Client(Region=self.spec.region)

            # Delete each uploaded file
            for file_key in uploaded_files:
                try:
                    client.delete_object(Bucket=self.spec.bucket_name, Key=file_key)
                    log.debug(f"Deleted file: s3://{self.spec.bucket_name}/{file_key}")
                except Exception as e:
                    log.warning(f"Failed to delete file {file_key}: {str(e)}")

            log.info(f"Successfully removed {len(uploaded_files)} context files from S3")

        except Exception as e:
            log.warning(f"Failed to remove context files during unexecute: {str(e)}")
            # Don't fail the unexecute operation for cleanup issues

        log.trace("UploadContextAction._unexecute() complete")

    def _cancel(self):
        """No-op; context upload cannot be cancelled."""
        log.debug("Cancel requested for context upload - operation cannot be cancelled")

    def _resolve(self):
        """Render templates and normalize parameters for execution."""
        log.trace("UploadContextAction._resolve()")

        try:
            # Render template variables
            self.spec.account = self.renderer.render_string(self.spec.account, self.context)
            self.spec.bucket_name = self.renderer.render_string(self.spec.bucket_name, self.context)
            self.spec.region = self.renderer.render_string(self.spec.region, self.context)
            self.spec.prefix = self.renderer.render_string(self.spec.prefix, self.context)

            # Clean up prefix (remove leading/trailing slashes)
            self.spec.prefix = self.spec.prefix.strip("/")

            log.debug(f"Resolved context upload to s3://{self.spec.bucket_name}/{self.spec.prefix}/")

        except Exception as e:
            error_message = f"Failed to resolve template variables: {str(e)}"
            log.error(error_message)
            self.set_failed(error_message)

        log.trace("UploadContextAction._resolve() complete")

    @classmethod
    def generate_action_resource(cls, **kwargs) -> UploadContextActionResource:
        """Factory: create a typed UploadContextActionResource."""
        return UploadContextActionResource.model_validate(kwargs)

    @classmethod
    def generate_action_parameters(cls, **kwargs) -> UploadContextActionSpec:
        """Factory: create typed UploadContextActionSpec."""
        return UploadContextActionSpec.model_validate(kwargs)
