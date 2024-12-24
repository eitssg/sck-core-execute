"""Share an image with other accounts"""

from typing import Any

import core_logging as log

from core_framework.models import ActionDefinition, DeploymentDetails, ActionParams

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


def generate_template() -> ActionDefinition:
    """Generate the action definition"""

    definition = ActionDefinition(
        Label="action-definition-label",
        Type="AWS::ShareImage",
        DependsOn=["put-a-label-here"],
        Params=ActionParams(
            Account="The account to use for the action (required)",
            Region="The region to create the stack in (required)",
            ImageName="The name of the image to share (required)",
            AccountsToShare=["The accounts to share the image with (required)"],
            Siblings=["The accounts that are allowed to share the image (required)"],
        ),
        Scope="Based on your deployment details, it one of 'portfolio', 'app', 'branch', or 'build'",
    )

    return definition


class ShareImageAction(BaseAction):
    """Share an image with other accounts.

    This action will share an image with other accounts.  The action will wait for the sharing to complete before returning.

    Attributes:
        Type: Use the value: ``AWS::ShareImage``
        Params.Account: The account where the image is located
        Params.Region: The region where the image is located
        Params.ImageName: The name of the image to share (required)
        Params.AccountsToShare: The accounts to share the image with (required)
        Params.Siblings: The accounts that are allowed to share the image (required)

    .. rubric: ActionDefinition:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Label: action-aws-shareimage-label
              Type: "AWS::ShareImage"
              Params:
                Account: "154798051514"
                Region: "ap-southeast-1"
                ImageName: "my-image-to-share"
                AccountsToShare: ["123456789012", "234567890123"]
                Siblings: ["123456789012", "234567890123"]
              Scope: "build"

    """

    def __init__(
        self,
        definition: ActionDefinition,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        if self.params.Tags is None:
            self.params.Tags = {}
        if deployment_details.DeliveredBy:
            self.params.Tags["DeliveredBy"] = deployment_details.DeliveredBy

    def _execute(self):

        log.trace("ShareImageAction._execute()")

        target_accounts = self.params.AccountsToShare

        # Obtain an EC2 client
        ec2_client = aws.ec2_client(
            region=self.params.Region,
            role=util.get_provisioning_role_arn(self.params.Account),
        )

        log.debug("Finding image with name '{}'", self.params.ImageName)

        # Find image (provides image id and snapshot ids)
        response = ec2_client.describe_images(
            Filters=[{"Name": "name", "Values": [self.params.ImageName]}]
        )

        if len(response["Images"]) == 0:
            self.set_complete(
                "Could not find image with name '{}'. It may have been previously deleted.".format(
                    self.params.ImageName
                )
            )
            log.warning(
                "Could not find image with name '{}'. It may have been previously deleted.",
                self.params.ImageName,
            )
            return

        for target in self.params.AccountsToShare:
            if target not in self.params.Siblings:
                self.set_failed(
                    "Sharing to account {} that is not permissible in accounts.yaml, you need to have AwsSiblings property containing list of account you may share this image to"
                )
                log.warning(
                    "Sharing to account {} that is not permissible in accounts.yaml, you need to have AwsSiblings property containing list of account you may share this image to"
                )
                return

        image_id = response["Images"][0]["ImageId"]

        log.debug("Found image '{}' with name '{}'", image_id, self.params.ImageName)

        ec2_client.modify_image_attribute(
            ImageId=image_id,
            LaunchPermission={
                "Add": list(
                    map(
                        lambda s: {
                            "UserId": s,
                        },
                        self.params.AccountsToShare,
                    )
                ),
            },
        )

        log.debug(
            "Successfully shared AMI {} to target account {}", image_id, target_accounts
        )
        self.set_complete()

        log.trace("ShareImageAction._execute()")

    def _check(self):
        pass

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):

        log.trace("ShareImageAction._resolve()")

        self.params.Account = self.renderer.render_string(
            self.params.Account, self.context
        )
        self.params.ImageName = self.renderer.render_string(
            self.params.ImageName, self.context
        )
        self.params.Region = self.renderer.render_string(
            self.params.Region, self.context
        )

        log.trace("ShareImageAction._resolve() complete")
