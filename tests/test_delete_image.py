from typing import Any
import traceback
from unittest import mock
from webbrowser import get
import pytest
from unittest.mock import MagicMock
from datetime import datetime, timezone
from botocore.exceptions import ClientError

from core_framework.models import TaskPayload, DeploySpec

from core_execute.actionlib.actions.aws.delete_image import (
    DeleteImageActionResource,
    DeleteImageActionSpec,
)
from core_execute.handler import handler as execute_handler
from core_execute.execute import save_actions, save_state, load_state

from .aws_fixtures import *

action_name = "deleteimage-test"


# Scope this so it's created fresh for each test
@pytest.fixture
def task_payload() -> TaskPayload:
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
    return TaskPayload.model_validate(data)


@pytest.fixture
def deploy_spec() -> DeploySpec:
    """
    Fixture to provide a deployspec data for testing.
    This can be used to mock the deployspec in tests.
    Parameters are fore: DeleteImageActionSpec
    """
    spec_params = {
        "Account": "154798051514",
        "Region": "ap-southeast-1",
        "ImageName": "my-image-name",
    }

    spec = DeleteImageActionSpec.model_validate(spec_params)

    action_resource = DeleteImageActionResource(name=action_name, spec=spec)

    return DeploySpec(actions=[action_resource])


def test_delete_image_action(task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session):

    try:

        creation_time = datetime(2023, 10, 1, 12, 0, 0, tzinfo=timezone.utc)

        mock_client = mock_session().client(
            'ec2',
            region_name="ap-southeast-1",
            **get_role_credentials(
                RoleArn=util.get_provisioning_role_arn("154798051514"),
            ),
        )

        # Mock describe_images - returns image info before deletion
        mock_client.describe_images.return_value = {
            "Images": [
                {
                    "ImageId": "ami-1234567890abcdef0",
                    "Name": "my-image-name",
                    "Description": "My custom AMI image",
                    "Architecture": "x86_64",
                    "State": "available",
                    "CreationDate": creation_time.isoformat(),
                    "Public": False,
                    "OwnerId": "154798051514",
                    "ImageType": "machine",
                    "RootDeviceName": "/dev/sda1",
                    "RootDeviceType": "ebs",
                    "VirtualizationType": "hvm",
                    "BlockDeviceMappings": [
                        {
                            "DeviceName": "/dev/sda1",
                            "Ebs": {
                                "SnapshotId": "snap-1234567890abcdef0",
                                "VolumeSize": 20,
                                "VolumeType": "gp3",
                                "DeleteOnTermination": True,
                                "Encrypted": False,
                            },
                        },
                        {
                            "DeviceName": "/dev/sdb",
                            "Ebs": {
                                "SnapshotId": "snap-0987654321fedcba0",
                                "VolumeSize": 100,
                                "VolumeType": "gp3",
                                "DeleteOnTermination": False,
                                "Encrypted": True,
                            },
                        },
                    ],
                    "Tags": [
                        {"Key": "Name", "Value": "my-image-name"},
                        {"Key": "Environment", "Value": "production"},
                    ],
                }
            ]
        }

        # Mock deregister_image - returns successful deregistration
        mock_client.deregister_image.return_value = {
            "ResponseMetadata": {
                "RequestId": "12345678-1234-1234-1234-123456789012",
                "HTTPStatusCode": 200,
            }
        }

        # Mock delete_snapshot - returns successful deletion for both snapshots
        mock_client.delete_snapshot.return_value = {
            "ResponseMetadata": {
                "RequestId": "12345678-1234-1234-1234-123456789012",
                "HTTPStatusCode": 200,
            }
        }

        save_actions(task_payload, deploy_spec.actions)
        save_state(task_payload, {})

        # Execute the action
        event = task_payload.model_dump()
        result = execute_handler(event, None)

        assert result is not None, "Result should not be None"
        assert isinstance(result, dict), "Result should be a dictionary"

        task_payload = TaskPayload.model_validate(result)

        # Validate the flow control in the task payload
        assert task_payload.flow_control == "success", f"Expected flow_control to be 'success', got '{task_payload.flow_control}'"

        state = load_state(task_payload)

        # Verify EC2 client method calls
        mock_client.describe_images.assert_called_once_with(Filters=[{"Name": "name", "Values": ["my-image-name"]}])

        mock_client.deregister_image.assert_called_once_with(ImageId="ami-1234567890abcdef0")

        # Should be called twice for both snapshots
        assert mock_client.delete_snapshot.call_count == 2
        snapshot_calls = [call.kwargs for call in mock_client.delete_snapshot.call_args_list]
        snapshot_ids = [call["SnapshotId"] for call in snapshot_calls]
        assert "snap-1234567890abcdef0" in snapshot_ids
        assert "snap-0987654321fedcba0" in snapshot_ids

        # Check basic parameters are stored in state
        assert f"var/{action_name}/ImageName" in state
        assert state[f"var/{action_name}/ImageName"] == "my-image-name"

        assert f"var/{action_name}/Region" in state
        assert state[f"var/{action_name}/Region"] == "ap-southeast-1"

        assert f"var/{action_name}/Account" in state
        assert state[f"var/{action_name}/Account"] == "154798051514"

        # Check deletion operation tracking
        assert f"var/{action_name}/DeletionStarted" in state
        assert state[f"var/{action_name}/DeletionStarted"] is True

        assert f"var/{action_name}/DeletionCompleted" in state
        assert state[f"var/{action_name}/DeletionCompleted"] is True

        assert f"var/{action_name}/DeletionResult" in state
        assert state[f"var/{action_name}/DeletionResult"] == "SUCCESS"

        assert f"var/{action_name}/ImageExists" in state
        assert state[f"var/{action_name}/ImageExists"] is True

        # Check image metadata captured before deletion
        assert f"var/{action_name}/ImageId" in state
        assert state[f"var/{action_name}/ImageId"] == "ami-1234567890abcdef0"

        assert f"var/{action_name}/ImageDescription" in state
        assert state[f"var/{action_name}/ImageDescription"] == "My custom AMI image"

        assert f"var/{action_name}/ImageArchitecture" in state
        assert state[f"var/{action_name}/ImageArchitecture"] == "x86_64"

        assert f"var/{action_name}/ImageState" in state
        assert state[f"var/{action_name}/ImageState"] == "available"

        assert f"var/{action_name}/ImageCreationDate" in state
        assert state[f"var/{action_name}/ImageCreationDate"] == creation_time

        # Check image deregistration
        assert f"var/{action_name}/ImageDeregistered" in state
        assert state[f"var/{action_name}/ImageDeregistered"] is True

        # Check snapshot information
        assert f"var/{action_name}/SnapshotIds" in state
        snapshot_ids_state = state[f"var/{action_name}/SnapshotIds"]
        assert "snap-1234567890abcdef0" in snapshot_ids_state
        assert "snap-0987654321fedcba0" in snapshot_ids_state

        assert f"var/{action_name}/SnapshotCount" in state
        assert state[f"var/{action_name}/SnapshotCount"] == 2

        assert f"var/{action_name}/DeletedSnapshots" in state
        deleted_snapshots = state[f"var/{action_name}/DeletedSnapshots"]
        assert len(deleted_snapshots) == 2
        assert "snap-1234567890abcdef0" in deleted_snapshots
        assert "snap-0987654321fedcba0" in deleted_snapshots

        assert f"var/{action_name}/DeletedSnapshotCount" in state
        assert state[f"var/{action_name}/DeletedSnapshotCount"] == 2

        assert f"var/{action_name}/FailedSnapshotCount" in state
        assert state[f"var/{action_name}/FailedSnapshotCount"] == 0

        # Check timing information
        assert f"var/{action_name}/StartTime" in state
        assert f"var/{action_name}/CompletionTime" in state

        # Check status
        assert f"{action_name}/StatusCode" in state
        assert state[f"{action_name}/StatusCode"] == "complete"

        print("All AMI image deletion validations passed")
        print(f"Image: {state.get(f'var/{action_name}/ImageName')}")
        print(f"Image ID: {state.get(f'var/{action_name}/ImageId')}")
        print(f"Deletion Result: {state.get(f'var/{action_name}/DeletionResult')}")
        print(f"Snapshots Deleted: {state.get(f'var/{action_name}/DeletedSnapshotCount')}")

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
        pytest.fail(f"Test failed due to an exception: {e}")


def test_delete_image_not_found(task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session):
    """Test deletion of an image that doesn't exist."""

    reset()

    try:

        # if mock_session.client mock already has a describe_images method, then update its
        # return value else we creeate a describe_images mock method

        mock_client = mock_session().client(
            'ec2',
            region_name="ap-southeast-1",
            **get_role_credentials(
                RoleArn=util.get_provisioning_role_arn("154798051514"),
            ),
        )

        mock_client.describe_images.return_value = {"Images": []}

        save_actions(task_payload, deploy_spec.actions)
        save_state(task_payload, {})

        event = task_payload.model_dump()
        result = execute_handler(event, None)

        assert result is not None, "Result should not be None"
        task_payload = TaskPayload.model_validate(result)

        # Should still succeed when image doesn't exist
        assert task_payload.flow_control == "success", f"Expected flow_control to be 'success', got '{task_payload.flow_control}'"

        state = load_state(task_payload)

        # Check that image was marked as not existing
        assert f"var/{action_name}/ImageExists" in state
        assert state[f"var/{action_name}/ImageExists"] is False

        assert f"var/{action_name}/DeletionResult" in state
        assert state[f"var/{action_name}/DeletionResult"] == "NOT_FOUND"

        assert f"var/{action_name}/DeletionCompleted" in state
        assert state[f"var/{action_name}/DeletionCompleted"] is True

        print("Image not found test passed")

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
        pytest.fail(f"Test failed due to an exception: {e}")


def test_delete_image_deregistration_error(task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session):
    """Test image deregistration failure scenario."""

    reset()

    try:
        creation_time = datetime(2023, 10, 1, 12, 0, 0, tzinfo=timezone.utc)

        mock_client = mock_session().client(
            'ec2',
            region_name="ap-southeast-1",
            **get_role_credentials(
                RoleArn=util.get_provisioning_role_arn("154798051514"),
            ),
        )

        # Mock describe_images - image exists
        mock_client.describe_images.return_value = {
            "Images": [
                {
                    "ImageId": "ami-1234567890abcdef0",
                    "Name": "my-image-name",
                    "Description": "My custom AMI image",
                    "Architecture": "x86_64",
                    "State": "available",
                    "CreationDate": creation_time.isoformat(),
                    "BlockDeviceMappings": [
                        {
                            "DeviceName": "/dev/sda1",
                            "Ebs": {
                                "SnapshotId": "snap-1234567890abcdef0",
                                "VolumeSize": 20,
                                "VolumeType": "gp3",
                            },
                        }
                    ],
                }
            ]
        }

        # Mock deregister_image - fails with access denied
        mock_client.deregister_image.side_effect = ClientError(
            error_response={
                "Error": {
                    "Code": "UnauthorizedOperation",
                    "Message": "You are not authorized to perform this operation",
                }
            },
            operation_name="DeregisterImage",
        )

        save_actions(task_payload, deploy_spec.actions)
        save_state(task_payload, {})

        event = task_payload.model_dump()
        result = execute_handler(event, None)

        assert result is not None, "Result should not be None"
        task_payload = TaskPayload.model_validate(result)

        # Should fail when deregistration encounters an error
        assert task_payload.flow_control == "failure", f"Expected flow_control to be 'failure', got '{task_payload.flow_control}'"

        state = load_state(task_payload)

        # Check that image was found but deregistration failed
        assert f"var/{action_name}/ImageExists" in state
        assert state[f"var/{action_name}/ImageExists"] is True

        assert f"var/{action_name}/ImageDeregistrationFailed" in state
        assert state[f"var/{action_name}/ImageDeregistrationFailed"] is True

        assert f"var/{action_name}/DeregistrationFailureReason" in state
        assert "UnauthorizedOperation" in state[f"var/{action_name}/DeregistrationFailureReason"]

        print("Image deregistration error test passed")

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
        pytest.fail(f"Test failed due to an exception: {e}")
