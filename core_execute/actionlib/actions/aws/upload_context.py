from typing import Any

import core_helper.aws as aws
import json
import core_logging as log
import re
import core_framework as util
import yaml

from core_framework.models import ActionDefinition, DeploymentDetails

from core_execute.actionlib.action import BaseAction


class UploadContextAction(BaseAction):
    """
    Output the "Context" object (all output variables) to S3

    Creates a file called "context.yaml" on Se

    The "Context" is the FACTS output from the Factor API, which is a dictionary of all variables
    used in the generation of cloudformation templates

    """

    def __init__(
        self,
        definition: ActionDefinition,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        self.bucket = self.params.BucketName
        self.region = self.params.Region
        self.prefix = self.params.Prefix

    def __context_outputs(self):
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
                raise ValueError("Unsupported PRN format")

            outputs[var_name] = value
        return outputs

    def _execute(self):
        # Obtain an S3 client
        s3_client = aws.s3_client(region=self.region)

        # Upload context as YAML
        s3_key = "{}/context.yaml".format(self.prefix)

        log.debug("Uploading context file '{}' to '{}'", s3_key, self.bucket)

        body_hash = {}
        for key, value in self.__context_outputs().items():
            var_path = key.split("/")
            util.set_nested(body_hash, var_path, value)

        yaml_string = yaml.safe_dump(body_hash, default_flow_style=False)

        s3_client.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=yaml_string,
            ServerSideEncryption="AES256",
        )

        # Upload context as JSON
        s3_key = "{}/context.json".format(self.prefix)

        log.debug("Uploading context file '{}' to '{}'", s3_key, self.bucket)

        json_string = json.dumps(body_hash, indent=4)
        s3_client.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=json_string,
            ServerSideEncryption="AES256",
        )

        # Upload context as Bash exports
        s3_key = "{}/context.sh".format(self.prefix)

        log.debug("Uploading context file '{}' to '{}'", s3_key, self.bucket)

        body_array = []
        for key, value in self.__context_outputs().items():
            var_name = re.sub(r"[^a-zA-Z0-9]", "_", key)
            body_array.append('export {}="{}"'.format(var_name, value))
        bash_string = "\n".join(body_array)
        s3_client.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=bash_string,
            ServerSideEncryption="AES256",
        )

        self.set_complete()

    def _check(self):
        self.set_failed("Internal error - _check() should not have been called")

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):
        pass
