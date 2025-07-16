"""Share an image with other accounts"""

from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator

import core_logging as log

from core_framework.models import ActionSpec, DeploymentDetails

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction


class ShareImageActionParams(BaseModel):
    """Parameters for the ShareImageAction"""

    model_config = ConfigDict(populate_by_name=True, validate_assignment=True)

    account: str = Field(
        ..., alias="Account", description="The account to use for the action (required)"
    )
    region: str = Field(
        ..., alias="Region", description="The region to create the stack in (required)"
    )
    image_name: str = Field(
        ..., alias="ImageName", description="The name of the image to share (required)"
    )
    accounts_to_share: list[str] = Field(
        ...,
        alias="AccountsToShare",
        description="The accounts to share the image with (required)",
    )
    siblings: list[str] = Field(
        ...,
        alias="Siblings",
        description="The accounts that are allowed to share the image (required)",
    )
    tags: dict[str, str] | None = Field(
        default_factory=dict,
        alias="Tags",
        description="The tags to apply to the image (optional)",
    )


class ShareImageActionSpec(ActionSpec):
    """Generate the action definition"""

    @model_validator(mode="before")
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Validate the parameters for the ShareImageActionSpec"""
        if not (values.get("name") or values.get("Name")):
            values["name"] = "action-aws-shareimage-name"
        if not (values.get("kind") or values.get("Kind")):
            values["kind"] = "AWS::ShareImage"
        if not (values.get("depends_on") or values.get("DependsOn")):
            values["depends_on"] = []
        if not (values.get("scope") or values.get("Scope")):
            values["scope"] = "build"
        if not (values.get("params") or values.get("Params")):
            values["params"] = {
                "account": "",
                "region": "",
                "image_name": "",
                "accounts_to_share": [],
                "siblings": [],
            }
        return values


class ShareImageAction(BaseAction):
    """Share an image with other accounts.

    This action will share an image with other accounts.  The action will wait for the sharing to complete before returning.

    Attributes:
        Kind: Use the value: ``AWS::ShareImage``
        Params.Account: The account where the image is located
        Params.Region: The region where the image is located
        Params.ImageName: The name of the image to share (required)
        Params.AccountsToShare: The accounts to share the image with (required)
        Params.Siblings: The accounts that are allowed to share the image (required)

    .. rubric: ActionSpec:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Name: action-aws-shareimage-name
              Kind: "AWS::ShareImage"
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
        definition: ActionSpec,
        context: dict[str, Any],
        deployment_details: DeploymentDetails,
    ):
        super().__init__(definition, context, deployment_details)

        # Validate the action parameters
        self.params = ShareImageActionParams(**definition.params)

        if deployment_details.delivered_by:
            self.params.tags["DeliveredBy"] = deployment_details.delivered_by

    def _execute(self):

        log.trace("ShareImageAction._execute()")

        target_accounts = self.params.accounts_to_share

        # Obtain an EC2 client
        ec2_client = aws.ec2_client(
            region=self.params.region,
            role=util.get_provisioning_role_arn(self.params.account),
        )

        log.debug("Finding image with name '{}'", self.params.image_name)

        # Find image (provides image id and snapshot ids)
        response = ec2_client.describe_images(
            Filters=[{"Name": "name", "Values": [self.params.image_name]}]
        )

        if len(response["Images"]) == 0:
            self.set_complete(
                "Could not find image with name '{}'. It may have been previously deleted.".format(
                    self.params.image_name
                )
            )
            log.warning(
                "Could not find image with name '{}'. It may have been previously deleted.",
                self.params.image_name,
            )
            return

        for target in self.params.accounts_to_share:
            if target not in self.params.siblings:
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
                        self.params.accounts_to_share,
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

        self.params.account = self.renderer.render_string(
            self.params.account, self.context
        )
        self.params.image_name = self.renderer.render_string(
            self.params.image_name, self.context
        )
        self.params.region = self.renderer.render_string(
            self.params.region, self.context
        )

        log.trace("ShareImageAction._resolve() complete")
