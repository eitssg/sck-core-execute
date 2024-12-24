"""Upload the Jinja2 Render context to the appropriate S3 bucket"""

from typing import Any
import re
import json
import yaml

import core_logging as log

import core_helper.aws as aws

import core_framework as util

from core_framework.models import ActionDefinition, DeploymentDetails, ActionParams

from core_execute.actionlib.action import BaseAction


def generate_template() -> ActionDefinition:
    """Generate the action definition"""

    definition = ActionDefinition(
        Label="action-definition-label",
        Type="AWS::UnprotectELBAction",
        DependsOn=["put-a-label-here"],
        Params=ActionParams(
            Account="The account to use for the action (required)",
            BucketName="The name of the bucket to upload the context to (required)",
            Region="The region to create the stack in (required)",
            Prefix="The prefix to use for the context file (required)",
        ),
        Scope="Based on your deployment details, it one of 'portfolio', 'app', 'branch', or 'build'",
    )

    return definition


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

    .. rubric: ActionDefinition:

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
        definition: ActionDefinition,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

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
        s3_client = aws.s3_client(region=self.params.Region)

        # Upload context as YAML
        s3_key = "{}/context.yaml".format(self.params.Prefix)

        log.debug("Uploading context file '{}' to '{}'", s3_key, self.params.BucketName)

        body_hash = {}
        for key, value in self.__context_outputs().items():
            var_path = key.split("/")
            util.set_nested(body_hash, var_path, value)

        yaml_string = yaml.safe_dump(body_hash, default_flow_style=False)

        s3_client.put_object(
            Bucket=self.params.BucketName,
            Key=s3_key,
            Body=yaml_string,
            ServerSideEncryption="AES256",
        )

        # Upload context as JSON
        s3_key = "{}/context.json".format(self.params.Prefix)

        log.debug("Uploading context file '{}' to '{}'", s3_key, self.params.BucketName)

        json_string = json.dumps(body_hash, indent=4)
        s3_client.put_object(
            Bucket=self.params.BucketName,
            Key=s3_key,
            Body=json_string,
            ServerSideEncryption="AES256",
        )

        # Upload context as Bash exports
        s3_key = "{}/context.sh".format(self.params.Prefix)

        log.debug("Uploading context file '{}' to '{}'", s3_key, self.params.BucketName)

        body_array = []
        for key, value in self.__context_outputs().items():
            var_name = re.sub(r"[^a-zA-Z0-9]", "_", key)
            body_array.append('export {}="{}"'.format(var_name, value))
        bash_string = "\n".join(body_array)
        s3_client.put_object(
            Bucket=self.params.BucketName,
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
