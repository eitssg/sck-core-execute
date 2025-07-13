"""Upload the Jinja2 Render context to the appropriate S3 bucket"""

from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator
import re
import json

import core_logging as log

import core_helper.aws as aws

import core_framework as util

from core_framework.models import ActionSpec, DeploymentDetails

from core_execute.actionlib.action import BaseAction


class UploadContextActionParams(BaseModel):
    """Parameters for the UnprotectELBAction"""

    model_config = ConfigDict(populate_by_name=True, validate_assignment=True)

    account: str = Field(
        ..., alias="Account", description="The account to use for the action (required)"
    )
    bucket_name: str = Field(
        ...,
        alias="BucketName",
        description="The name of the bucket to upload the context to (required)",
    )
    region: str = Field(
        ..., alias="Region", description="The region to create the stack in (required)"
    )
    prefix: str = Field(
        ...,
        alias="Prefix",
        description="The prefix to use for the context file (required)",
    )


class UploadContextActionSpec(ActionSpec):
    """Generate the action definition"""

    @model_validator(mode="before")
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Validate the parameters for the UnprotectELBActionSpec"""
        if not (values.get("label") or values.get("Label")):
            values["label"] = "action-aws-unprotect-elb-label"
        if not (values.get("type") or values.get("Type")):
            values["type"] = "AWS::UnprotectELBAction"
        if not (values.get("depends_on") or values.get("DependsOn")):
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Params")):
            values["params"] = {
                "account": "",
                "bucket_name": "",
                "region": "",
                "prefix": "",
            }
        return values


class UploadContextAction(BaseAction):
    """
    Output the "Context" object (all output variables) to S3

    Creates a file called "context.yaml" on Se

    The "Context" is the FACTS output from the Factor API, which is a dictionary of all variables
    used in the generation of cloudformation templates

    Attributes:
        Type: Use the value: ``AWS::UploadContext``
        Params.Account: The account where the bucket is located
        Params.Region: The region where the bucket is located
        Params.BucketName: The name of the bucket to upload the context to (required)
        Params.Prefix: The prefix to use for the context file (required)

    .. rubric: ActionSpec:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Label: action-aws-uploadcontext-label
              Type: "AWS::UploadContext"
              Params:
                Account: "154798051514"
                BucketName: "my-bucket-name"
                Region: "ap-southeast-1"
                Prefix: "my-prefix"
              Scope: "build"

    """

    def __init__(
        self,
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        self.params = UploadContextActionParams(**definition.params)

    def __context_outputs(self):

        log.trace("UploadContextAction.__context_outputs()")

        outputs = {}

        for key, value in self.context.items():
            prn, label = key.split("/", 1)

            # Only upload output values (not vars or action state)
            if not prn.endswith(":output"):
                continue

            # Example PRN is prn:demo:ecs:testing:2:web2:action/deploy/main/upload-context
            if prn.count(":") == 6:
                # Component PRN
                _, portfolio, app, branch, build, component, resource_type = prn.split(
                    ":"
                )
                var_name = "{}/{}".format(component, label)
            elif prn.count(":") == 5:
                # App PRN
                _, portfolio, app, branch, build, resource_type = prn.split(":")
                var_name = "{}/{}".format("pipeline", label)
            else:
                log.fatal("Unsupported PRN format")
                raise ValueError("Unsupported PRN format")

            outputs[var_name] = value

        log.trace("Context outputs: {}", outputs)

        return outputs

    def _execute(self):

        log.trace("UploadContextAction._execute()")

        # Obtain an S3 client
        s3_client = aws.s3_client(region=self.params.region)

        # Upload context as YAML
        s3_key = "{}/context.yaml".format(self.params.prefix)

        log.debug(
            "Uploading context file '{}' to '{}'", s3_key, self.params.bucket_name
        )

        body_hash = {}
        for key, value in self.__context_outputs().items():
            var_path = key.split("/")
            util.set_nested(body_hash, var_path, value)

        yaml_string = util.to_yaml(body_hash)

        # TODO - change to MagicS3Client

        s3_client.put_object(
            Bucket=self.params.bucket_name,
            Key=s3_key,
            Body=yaml_string,
            ServerSideEncryption="AES256",
        )

        # Upload context as JSON
        s3_key = "{}/context.json".format(self.params.prefix)

        log.debug(
            "Uploading context file '{}' to '{}'", s3_key, self.params.bucket_name
        )

        # TODO - change to MagicS3Client

        json_string = json.dumps(body_hash, indent=4)
        s3_client.put_object(
            Bucket=self.params.bucket_name,
            Key=s3_key,
            Body=json_string,
            ServerSideEncryption="AES256",
        )

        # Upload context as Bash exports
        s3_key = "{}/context.sh".format(self.params.prefix)

        log.debug(
            "Uploading context file '{}' to '{}'", s3_key, self.params.bucket_name
        )

        body_array = []
        for key, value in self.__context_outputs().items():
            var_name = re.sub(r"[^a-zA-Z0-9]", "_", key)
            body_array.append('export {}="{}"'.format(var_name, value))
        bash_string = "\n".join(body_array)
        s3_client.put_object(
            Bucket=self.params.bucket_name,
            Key=s3_key,
            Body=bash_string,
            ServerSideEncryption="AES256",
        )

        self.set_complete()

        log.trace("UploadContextAction._execute() complete")

    def _check(self):

        log.trace("UploadContextAction._check()")

        self.set_failed("Internal error - _check() should not have been called")

        log.trace("UploadContextAction._check() complete")

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):
        pass
