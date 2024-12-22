from typing import Any

import core_logging as log

from core_framework.models import ActionDefinition, DeploymentDetails

import core_helper.aws as aws

import core_execute.envinfo as envinfo
from core_execute.actionlib.action import BaseAction


class ShareImageAction(BaseAction):

    def __init__(
        self,
        definition: ActionDefinition,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)
        self.account = self.params.Account
        self.image_name = self.params.ImageName
        self.region = self.params.Region
        self.accounts_to_share = self.params.AccountsToShare
        self.siblings = self.params.Siblings

        tags = self.params.Tags or {}
        if deployment_details.DeliveredBy:
            tags["DeliveredBy"] = deployment_details.DeliveredBy
        self.tags = aws.transform_tag_hash(tags)

    def _execute(self):
        target_accounts = self.accounts_to_share

        # Obtain an EC2 client
        ec2_client = aws.ec2_client(
            region=self.region, role=envinfo.provisioning_role_arn(self.account)
        )

        log.debug("Finding image with name '{}'", self.image_name)

        # Find image (provides image id and snapshot ids)
        response = ec2_client.describe_images(
            Filters=[{"Name": "name", "Values": [self.image_name]}]
        )

        if len(response["Images"]) == 0:
            self.set_complete(
                "Could not find image with name '{}'. It may have been previously deleted.".format(
                    self.image_name
                )
            )
            return

        for target in self.accounts_to_share:
            if target not in self.siblings:
                self.set_failed(
                    "Sharing to account {} that is not permissible in accounts.yaml, you need to have AwsSiblings property containing list of account you may share this image to"
                )
                return

        image_id = response["Images"][0]["ImageId"]

        log.debug("Found image '{}' with name '{}'", image_id, self.image_name)

        ec2_client.modify_image_attribute(
            ImageId=image_id,
            LaunchPermission={
                "Add": list(
                    map(
                        lambda s: {
                            "UserId": s,
                        },
                        self.accounts_to_share,
                    )
                ),
            },
        )

        log.debug(
            "Successfully shared AMI {} to target account {}", image_id, target_accounts
        )
        self.set_complete()

    def _check(self):
        pass

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):
        self.account = self.renderer.render_string(self.account, self.context)
        self.image_name = self.renderer.render_string(self.image_name, self.context)
        self.region = self.renderer.render_string(self.region, self.context)
