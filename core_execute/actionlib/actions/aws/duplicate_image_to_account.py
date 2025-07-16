"""Duplicate an Image and copy it to one ore more accounts"""

from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator

import core_logging as log

from core_framework.models import DeploymentDetails, ActionSpec

import core_helper.aws as aws

import core_framework as util
from core_execute.actionlib.action import BaseAction

import boto3


class DuplicateImageToAccountActionParams(BaseModel):
    """Parameters for the DuplicateImageToAccountAction"""

    model_config = ConfigDict(populate_by_name=True, validate_assignment=True)

    account: str = Field(
        ...,
        alias="Account",
        description="The account to use for the action (required)",
    )
    region: str = Field(
        ...,
        alias="Region",
        description="The region to create the stack in (required)",
    )
    image_name: str = Field(
        ...,
        alias="ImageName",
        description="The name of the image to duplicate (required)",
    )
    accounts_to_share: list[str] = Field(
        ...,
        alias="AccountsToShare",
        description="The accounts to share the image with (required)",
    )
    kms_key_arn: str = Field(
        ...,
        alias="KmsKeyArn",
        description="The KMS key ARN to use for encryption (required)",
    )
    tags: dict[str, str] | None = Field(
        default_factory=dict,
        alias="Tags",
        description="The tags to apply to the image (optional)",
    )


class DuplicateImageToAccountActionSpec(ActionSpec):
    """Generate the action definition"""

    @model_validator(mode="before")
    def validate_params(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Validate the parameters for the DuplicateImageToAccountActionSpec"""
        if not (values.get("name") or values.get("Name")):
            values["name"] = "action-aws-duplicateimagetoaccount-name"
        if not (values.get("kind") or values.get("Kind")):
            values["kind"] = "AWS::DuplicateImageToAccount"
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
                "kms_key_arn": "",
                "tags": {},
            }

        return values


class DuplicateImageToAccountAction(BaseAction):
    """Duplicate an Image and copy it to one ore more accounts

    This action will duplicate an image and copy it to one or more accounts.  The action will wait for the copy to complete before returning.

    Attributes:
        Kind: Use the value: ``AWS::DuplicateImageToAccount``
        Params.Account: The account where the image is located
        Params.Region: The region where the image is located
        Params.ImageName: The name of the image to duplicate (required)
        Params.AccountsToShare: The accounts to share the image with (required)
        Params.KmsKeyArn: The KMS key ARN to use for encryption (required)
        Params.Tags: The tags to apply to the image (optional)

    .. rubric: ActionSpec:

    .. tip:: s3:/<bucket>/artfacts/<deployment_details>/{task}.actions:

        .. code-block:: yaml

            - Name: action-aws-duplicateimagetoaccount-name
              Kind: "AWS::DuplicateImageToAccount"
              Params:
                Account: "154798051514"
                Region: "ap-southeast-1"
                Tags:
                    From: "John Smith"
                ImageName: "my-image-name"
                KmsKeyArn: "arn:aws:kms:ap-southeast-1:154798051514:key/your-kms-key-id"
                AccountsToShare: ["123456789012", "123456789013"]
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
        self.params = DuplicateImageToAccountActionParams(**definition.params)

        if deployment_details.delivered_by:
            self.params.tags["DeliveredBy"] = deployment_details.delivered_by

    def _execute(self):

        log.trace("DuplicateImageToAccountAction._execute()")

        if not self.params.accounts_to_share:
            self.set_complete("No accounts to share image with have been specified")
            log.warning("No accounts to share image with have been specified")
            return

        # Obtain an EC2 client
        ec2_client = aws.ec2_client(
            region=self.params.region,
            role=util.get_provisioning_role_arn(self.params.account),
        )

        # Find image (provides image id and snapshot ids)
        log.debug("Finding image with name '{}'", self.params.image_name)
        response = ec2_client.describe_images(
            Filters=[{"Name": "name", "Values": [self.params.image_name]}]
        )

        if len(response["Images"]) == 0:
            self.set_complete(
                "Could not find image with name '{}'. It may have been previously deleted.".format(
                    self.params.ImageName
                )
            )
            log.warning("Could not find image with name '{}'", self.params.image_name)
            return

        image_id = response["Images"][0]["ImageId"]
        log.debug("Found image '{}' with name '{}'", image_id, self.params.image_name)

        # Find snapshots of the encrypted image
        snapshot_ids = []
        for block_device_mapping in response["Images"][0]["BlockDeviceMappings"]:
            if "Ebs" not in block_device_mapping:
                continue
            snapshot_ids.append(block_device_mapping["Ebs"]["SnapshotId"])

        log.debug("Image '{}' has snapshots: {}", image_id, snapshot_ids)

        snapshot_id = snapshot_ids[0]

        # Share snapshot with the target account
        target_account = self.params.AccountsToShare[0]
        self.set_running(
            "Sharing snapshot with the target account {}".format(target_account)
        )
        log.debug(
            "Account {} is getting the image shared, starting now!", target_account
        )
        response = ec2_client.modify_snapshot_attribute(
            Attribute="createVolumePermission",
            OperationType="add",
            SnapshotId=snapshot_id,
            UserIds=[
                target_account,
            ],
        )
        log.debug("Successfully shared snapshot with target account {}", target_account)

        # Set Target account Instance object
        target_ec2_session = self.__ec2_session()
        target_ec2 = target_ec2_session.resource("ec2")
        log.debug("Successfully set target ec2 instance object")

        # Create a copy of the shared snapshot on the target account
        self.set_running(
            "Copying snapshot with the target account {}".format(target_account)
        )

        shared_snapshot = target_ec2.Snapshot(snapshot_id)
        copy = shared_snapshot.copy(
            SourceRegion=self.params.region,
            Encrypted=True,
            KmsKeyId=self.params.kms_key_arn,
        )

        copied_snapshot = target_ec2.Snapshot(copy["SnapshotId"])
        copied_snapshot.wait_until_completed()

        log.debug(
            "Successfully copied from snapshot {} to snapshot {}",
            snapshot_id,
            copied_snapshot.snapshot_id,
        )

        # Create AMI from snapshot in the target account
        self.set_running("Creating AMI in the target account {}".format(target_account))

        response = target_ec2.register_image(
            Architecture="x86_64",
            RootDeviceName="/dev/sda1",
            BlockDeviceMappings=[
                {
                    "DeviceName": "/dev/sda1",
                    "Ebs": {
                        "DeleteOnTermination": True,
                        "SnapshotId": copied_snapshot.snapshot_id,
                        "VolumeSize": copied_snapshot.volume_size,
                        "VolumeType": "gp2",
                    },
                },
            ],
            Description="Image created from snapshot {}".format(
                copied_snapshot.snapshot_id
            ),
            Name=self.params.image_name,
            # Name='Image created from source image {} target snapshot {}'.format(self.params.ImageName, copied_snapshot.snapshot_id),
            VirtualizationType="hvm",
            EnaSupport=True,
        )
        r_image_id = response.id

        log.debug(
            "Successfully created AMI {} from the shared snapshot in the target account {}",
            r_image_id,
            target_account,
        )

        self.set_state("ImageId{}".format(target_account), r_image_id)

        log.trace("DuplicateImageToAccountAction._execute() completed")

    def _check(self):

        log.trace("DuplicateImageToAccountAction._check()")

        target_account = self.params.accounts_to_share
        # Obtain an EC2 client
        ec2_client = aws.ec2_client(
            region=self.params.region,
            role=util.get_provisioning_role_arn(target_account),
        )

        # Wait for image creation to complete / fail
        image_id = self.get_state("ImageId{}".format(target_account))
        if image_id is None:
            log.error(
                "Internal error - state variable ImageId should have been set during action execution"
            )
            self.set_failed("No image previously created - cannot continue")
            return

        log.debug("Checking availability of copied image {}", image_id)

        describe_images_response = ec2_client.describe_images(ImageIds=[image_id])

        if len(describe_images_response["Images"]) == 0:
            self.set_failed("No images found with id '{}'".format(image_id))
            log.warning("No images found with id '{}'", image_id)
            return

        state = describe_images_response["Images"][0]["State"]

        if state == "available":
            self.set_running("Tagging image '{}'".format(image_id))
            ec2_client.create_tags(
                Resources=[image_id], Tags=aws.transform_tag_hash(self.params.tags)
            )

            image_snapshots = self.__get_image_snapshots(describe_images_response)
            self.set_running(
                "Tagging image snapshots: {}".format(", ".join(image_snapshots))
            )
            if len(image_snapshots) > 0:
                ec2_client.create_tags(
                    Resources=image_snapshots,
                    Tags=aws.transform_tag_hash(self.params.tags),
                )
            self.set_complete("Image is in state '{}'".format(state))

        elif state == "pending":
            self.set_running("Image is in state '{}'".format(state))
        else:
            self.set_failed("Image is in state '{}'".format(state))

        log.trace("Duplicate Image to Account Action check completed")

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):

        log.trace("DuplicateImageToAccountAction._resolve()")

        self.params.account = self.renderer.render_string(
            self.params.account, self.context
        )
        self.params.image_name = self.renderer.render_string(
            self.params.image_name, self.context
        )
        self.params.region = self.renderer.render_string(
            self.params.region, self.context
        )

        log.trace("DuplicateImageToAccountAction._resolve() completed")

    def __get_image_snapshots(self, describe_images_response):

        log.trace("Getting image snapshots")

        snapshots = []
        for mapping in describe_images_response["Images"][0]["BlockDeviceMappings"]:
            if ("Ebs" in mapping) and ("SnapshotId" in mapping["Ebs"]):
                snapshots.append(mapping["Ebs"]["SnapshotId"])

        log.trace("Got image snapshots")

        return snapshots

    def __ec2_session(self):

        log.trace("DuplicateImageToAccountAction.__ec2_session()")

        target_account = self.params.accounts_to_share

        credentials = aws.assume_role(
            role=util.get_provisioning_role_arn(target_account),
            session_name="temp-session-{}".format(target_account),
        )

        log.debug("Getting session on the target account {}", target_account)

        session = boto3.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
        )

        log.trace("Got session on the target account")

        return session
