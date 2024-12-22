from typing import Any

import core_logging as log

from core_framework.models import DeploymentDetails, ActionDefinition

import core_helper.aws as aws

import core_execute.envinfo as envinfo
from core_execute.actionlib.action import BaseAction

import boto3


class DuplicateImageToAccountAction(BaseAction):

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
        self.account_to_share = self.params.AccountsToShare or []
        self.kms_key_arn = self.params.KmsKeyArn

        tags = self.params.Tags or {}
        if deployment_details.DeliveredBy:
            tags["DeliveredBy"] = deployment_details.DeliveredBy
        self.tags = aws.transform_tag_hash(tags)

    def _execute(self):

        if len(self.account_to_share) == 0:
            self.set_complete("No accounts to share image with")
            return

        # Obtain an EC2 client
        ec2_client = aws.ec2_client(
            region=self.region, role=envinfo.provisioning_role_arn(self.account)
        )

        # Find image (provides image id and snapshot ids)
        log.debug("Finding image with name '{}'", self.image_name)
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

        image_id = response["Images"][0]["ImageId"]
        log.debug("Found image '{}' with name '{}'", image_id, self.image_name)

        # Find snapshots of the encrypted image
        snapshot_ids = []
        for block_device_mapping in response["Images"][0]["BlockDeviceMappings"]:
            if "Ebs" not in block_device_mapping:
                continue
            snapshot_ids.append(block_device_mapping["Ebs"]["SnapshotId"])

        log.debug("Image '{}' has snapshots: {}", image_id, snapshot_ids)

        snapshot_id = snapshot_ids[0]

        # Share snapshot with the target account
        target_account = self.account_to_share[0]
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
            SourceRegion=self.region, Encrypted=True, KmsKeyId=self.kms_key_arn
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
            Name=self.image_name,
            # Name='Image created from source image {} target snapshot {}'.format(self.image_name, copied_snapshot.snapshot_id),
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

    def _check(self):
        target_account = self.account_to_share
        # Obtain an EC2 client
        ec2_client = aws.ec2_client(
            region=self.region, role=envinfo.provisioning_role_arn(target_account)
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
            return

        state = describe_images_response["Images"][0]["State"]

        if state == "available":
            self.set_running("Tagging image '{}'".format(image_id))
            ec2_client.create_tags(Resources=[image_id], Tags=self.tags)

            image_snapshots = self.__get_image_snapshots(describe_images_response)
            self.set_running(
                "Tagging image snapshots: {}".format(", ".join(image_snapshots))
            )
            if len(image_snapshots) > 0:
                ec2_client.create_tags(Resources=image_snapshots, Tags=self.tags)
            self.set_complete("Image is in state '{}'".format(state))

        elif state == "pending":
            self.set_running("Image is in state '{}'".format(state))
        else:
            self.set_failed("Image is in state '{}'".format(state))

    def _unexecute(self):
        pass

    def _cancel(self):
        pass

    def _resolve(self):
        self.account = self.renderer.render_string(self.account, self.context)
        self.image_name = self.renderer.render_string(self.image_name, self.context)
        self.region = self.renderer.render_string(self.region, self.context)

    def __get_image_snapshots(self, describe_images_response):
        snapshots = []
        for mapping in describe_images_response["Images"][0]["BlockDeviceMappings"]:
            if ("Ebs" in mapping) and ("SnapshotId" in mapping["Ebs"]):
                snapshots.append(mapping["Ebs"]["SnapshotId"])
        return snapshots

    def __ec2_session(self):
        target_account = self.account_to_share

        credentials = aws.assume_role(
            role=envinfo.provisioning_role_arn(target_account),
            session_name="temp-session-{}".format(target_account),
        )

        log.debug("Getting session on the target account {}", target_account)

        return boto3.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
        )
