from typing import Any
import traceback
from unittest import mock
from unittest.mock import MagicMock, patch  # Add patch import
import pytest
from datetime import datetime, timezone

from core_framework.models import TaskPayload, DeploySpec

from core_execute.actionlib.actions.aws.duplicate_image_to_account import (
    DuplicateImageToAccountActionSpec,
)
from core_execute.handler import handler as execute_handler
from core_execute.execute import save_actions, save_state, load_state

from .aws_fixtures import *


# Scope this so it's created fresh for each test
@pytest.fixture
def task_payload():
    """
    Fixture to provide a sample payload data for testing.
    This can be used to mock the payload in tests.
    """
    data = {
        "Task": "deploy",
        "DeploymentDetails": {
            "Client": "client",
            "Portfolio": "portfolio",
            "Environment": "production",
            "Scope": "portfolio",  # Test this execution with a scope of portfolio
            "DataCenter": "zone-1",  # name of the data center ('availability zone' in AWS)
        },
    }
    return TaskPayload(**data)


@pytest.fixture
def deploy_spec():
    """
    Fixture to provide a deployspec data for testing.
    This can be used to mock the deployspec in tests.
    Parameters are fore: DeleteImageActionParams
    """
    spec: dict[str, Any] = {
        "Spec": {
            "Account": "154798051514",
            "Region": "ap-southeast-1",
            "ImageName": "my-application-ami-v1.0",
            "AccountsToShare": ["123456789012", "123456789013"],
            "KmsKeyArn": "arn:aws:kms:ap-southeast-1:154798051514:key/your-kms-key-id",
            "Tags": {"From": "John Smith", "Purpose": "Cross-account deployment"},
        }
    }

    action_spec = DuplicateImageToAccountActionSpec(**spec)

    deploy_spec: dict[str, Any] = {"actions": [action_spec]}

    return DeploySpec(**deploy_spec)


@pytest.fixture
def mock_source_ec2_client():

    mock_source_ec2_client = MagicMock()
    # Mock the source AMI describe_images call
    mock_source_ec2_client.describe_images.return_value = {
        "Images": [
            {
                "ImageId": "ami-source123",
                "Name": "my-application-ami-v1.0",
                "Architecture": "x86_64",
                "RootDeviceName": "/dev/sda1",
                "VirtualizationType": "hvm",
                "EnaSupport": True,
                "SriovNetSupport": "simple",
                "BlockDeviceMappings": [
                    {
                        "DeviceName": "/dev/sda1",
                        "Ebs": {
                            "SnapshotId": "snap-source123",
                            "VolumeSize": 20,
                            "VolumeType": "gp3",
                            "DeleteOnTermination": True,
                            "Encrypted": False,
                        },
                    }
                ],
            }
        ]
    }

    mock_source_ec2_client.assume_role.return_value = {
        "Credentials": {
            "AccessKeyId": "SourceAccessKeyId",
            "SecretAccessKey": "SourceSecretAccessKey",
            "SessionToken": "SourceSessionToken",
            "Expiration": datetime.now(timezone.utc)
            + timedelta(hours=1),  # Set expiration to 1 hour from now
        }
    }

    # Mock snapshot sharing (modify_snapshot_attribute)
    mock_source_ec2_client.modify_snapshot_attribute.return_value = {
        "ResponseMetadata": {"HTTPStatusCode": 200}
    }

    return mock_source_ec2_client


@pytest.fixture
def mock_target_ec2_client():
    mock_target_ec2_client = MagicMock()

    # Mock target account describe_images for _check() method
    mock_target_ec2_client.describe_images.return_value = {
        "Images": [
            {
                "ImageId": "ami-target123",
                "State": "available",
                "Name": "my-application-ami-v1.0-copy-123456789012",
                "BlockDeviceMappings": [
                    {
                        "DeviceName": "/dev/sda1",
                        "Ebs": {
                            "SnapshotId": "snap-target123",
                            "VolumeSize": 20,
                            "VolumeType": "gp3",
                        },
                    }
                ],
            }
        ]
    }

    mock_target_ec2_client.assume_role.return_value = {
        "Credentials": {
            "AccessKeyId": "TargetAccessKeyId",
            "SecretAccessKey": "wJalExExampleKey",
            "SessionToken": "FwoGZXhZ2ExaW5nZXJzZXhhbXBsZQ==",
            "Expiration": datetime.now(timezone.utc)
            + timedelta(hours=1),  # Set expiration to 1 hour from now
        }
    }

    # Mock target EC2 client tagging
    mock_target_ec2_client.create_tags.return_value = {
        "ResponseMetadata": {"HTTPStatusCode": 200}
    }

    # Mock target account describe_images for _check() method
    return mock_target_ec2_client


def test_duplicate_image_to_account(
    task_payload: TaskPayload,
    deploy_spec: DeploySpec,
    mock_session,
    mock_source_ec2_client,
    mock_target_ec2_client,
):
    try:

        mock_shared_snapshot = MagicMock()
        mock_shared_snapshot.name = "snap-source123"
        mock_shared_snapshot.copy.return_value = {"SnapshotId": "snap-target123"}

        mock_copied_snapshot = MagicMock()
        mock_copied_snapshot.snapshot_id = "snap-target123"
        mock_copied_snapshot.volume_size = 20
        mock_copied_snapshot.state = "completed"
        mock_copied_snapshot.reload.return_value = None
        mock_copied_snapshot.wait_until_completed.return_value = None

        mock_source_snapshot = MagicMock()
        mock_source_snapshot.snapshot_id = "snap-source123"
        mock_source_snapshot.volume_size = 20
        mock_source_snapshot.state = "completed"

        mock_source_ec2_resource = MagicMock()
        mock_source_ec2_resource.Snapshot.return_value = mock_source_snapshot

        def mock_target_snapshot_side_effect(snapshot_id):
            if snapshot_id == "snap-source123":
                return mock_shared_snapshot
            else:
                return mock_copied_snapshot

        mock_target_ec2_client.register_image.return_value = {
            "ImageId": "ami-target123"
        }

        mock_target_ec2_resource = MagicMock()
        mock_target_ec2_resource.Snapshot.side_effect = mock_target_snapshot_side_effect

        # Use role_arn to determine which client/resource to return
        def session_client_side_effect(service_name, **kwargs):
            role_arn = kwargs.get("role_arn", None) or kwargs.get("role", None)
            access_key = kwargs.get("aws_access_key_id", None)

            if role_arn:
                for a in ["123456789012", "123456789013"]:
                    if a in role_arn:
                        return mock_target_ec2_client
            if access_key and access_key == "TargetAccessKeyId":
                return mock_target_ec2_client
            return mock_source_ec2_client

        def session_resource_side_effect(service_name, **kwargs):
            role_arn = kwargs.get("role_arn", None) or kwargs.get("role", None)
            access_key = kwargs.get("aws_access_key_id", None)

            if role_arn:
                for a in ["123456789012", "123456789013"]:
                    if a in role_arn:
                        return mock_target_ec2_resource
            if access_key and access_key == "TargetAccessKeyId":
                return mock_target_ec2_resource
            return mock_source_ec2_resource

        # Apply side effects to the mock_session
        mock_session.client.side_effect = session_client_side_effect
        mock_session.resource.side_effect = session_resource_side_effect

        # Mock _get_target_session to return the same session
        def mock_get_target_session_method(self, target_account):
            print(f"DEBUG: _get_target_session called for account: {target_account}")
            return mock_session

        with patch(
            "core_execute.actionlib.actions.aws.duplicate_image_to_account.DuplicateImageToAccountAction._get_target_session",
            mock_get_target_session_method,
        ):

            # Execute the test
            save_actions(
                task_payload, deploy_spec.actions
            )  # Fixed: use .actions instead of .action_specs
            save_state(task_payload, {})

            # Execute the handler
            event = task_payload.model_dump()
            response = execute_handler(event, None)

            # Parse response back to TaskPayload
            task_payload = TaskPayload(**response)

            # Should be "execute" since AMI duplication continues with _check()
            assert (
                task_payload.flow_control == "success"
            ), f"Expected flow_control to be 'success', got '{task_payload.flow_control}'"

            state = load_state(task_payload)
            assert state is not None, "Expected state to be loaded successfully"

            # Check action state keys
            action_name = "action-aws-duplicateimagetoaccount-name"
            assert f"{action_name}/DuplicationStarted" in state
            assert f"{action_name}/SourceImageId" in state
            assert f"{action_name}/SuccessfulAccounts" in state
            assert f"{action_name}/CreatedImages" in state

            # Verify the actual values
            source_image_id = state[f"{action_name}/SourceImageId"]
            successful_accounts = state[f"{action_name}/SuccessfulAccounts"]
            created_images = state[f"{action_name}/CreatedImages"]

            assert source_image_id == "ami-source123"
            assert "123456789012" in successful_accounts
            assert "123456789012" in created_images
            assert created_images["123456789012"] == "ami-target123"

            # Verify AWS API calls were made correctly
            mock_source_ec2_client.describe_images.assert_called()
            mock_source_ec2_client.modify_snapshot_attribute.assert_called()
            mock_target_ec2_client.register_image.assert_called()

            print(
                "✅ test_duplicate_image_to_account passed - AMI duplicated successfully"
            )

    except Exception as e:
        traceback.print_exc()
        pytest.fail(f"Test failed due to an exception: {e}")
