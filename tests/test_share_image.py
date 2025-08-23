from typing import Any
import traceback
import pytest
from unittest.mock import MagicMock

import core_framework as util

from core_framework.models import TaskPayload, DeploySpec

from core_execute.actionlib.actions.aws.share_image import (
    ShareImageActionSpec,
    ShareImageActionParams,
)
from core_execute.handler import handler as execute_handler
from core_execute.execute import save_actions, save_state, load_state

from .aws_fixtures import *


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
    """
    validated_params = ShareImageActionParams(
        **{
            "Account": "1234567890123",
            "Region": util.get_region(),
            "ImageName": "ami-1234567890abcdef0",
            "AccountsToShare": ["123456789012", "098765432109"],
            "Siblings": ["123456789012", "098765432109"],
            "Tags": {"Environment": "production", "Project": "test-project"},
        }
    )

    action_spec = ShareImageActionSpec(Params=validated_params.model_dump())

    return DeploySpec(actions=[action_spec])


def test_share_image(task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session):
    """Test the share image action successful execution."""
    try:
        # Create mock EC2 client with proper method implementations
        mock_client = MagicMock()

        # Mock describe_images to return a found AMI
        mock_client.describe_images.return_value = {
            "Images": [
                {
                    "ImageId": "ami-1234567890abcdef0",
                    "Name": "ami-1234567890abcdef0",
                    "State": "available",
                    "OwnerId": "1234567890123",
                }
            ]
        }

        # Mock modify_image_attribute to simulate successful permission modification
        mock_client.modify_image_attribute.return_value = {
            "ResponseMetadata": {
                "RequestId": "test-request-id-123",
                "HTTPStatusCode": 200,
            }
        }

        mock_session.client.return_value = mock_client

        save_actions(task_payload, deploy_spec.action_specs)
        save_state(task_payload, {})

        # Execute the handler
        event = task_payload.model_dump()
        response = execute_handler(event, None)

        # Verify the response
        assert response is not None, "Response should not be None"
        assert isinstance(response, dict), "Response should be a dictionary"

        # Parse the response back into TaskPayload
        updated_payload = TaskPayload(**response)
        assert (
            updated_payload.flow_control == "success"
        ), "Flow control should be success"

        # Load the saved state to verify completion
        state = load_state(updated_payload)

        namespace = "share-image"

        # Verify EC2 describe_images was called correctly
        mock_client.describe_images.assert_called_once_with(
            Filters=[{"Name": "name", "Values": ["ami-1234567890abcdef0"]}]
        )

        # Verify modify_image_attribute was called correctly
        mock_client.modify_image_attribute.assert_called_once()
        modify_call_args = mock_client.modify_image_attribute.call_args

        # Check the ImageId parameter
        assert modify_call_args[1]["ImageId"] == "ami-1234567890abcdef0"

        # Check the LaunchPermission parameter
        launch_permission = modify_call_args[1]["LaunchPermission"]
        assert "Add" in launch_permission
        added_users = launch_permission["Add"]
        assert len(added_users) == 2, "Should add permissions for 2 accounts"

        # Verify the correct account IDs were added
        added_account_ids = [user["UserId"] for user in added_users]
        assert "123456789012" in added_account_ids
        assert "098765432109" in added_account_ids

        # Verify state tracking
        assert state is not None, "State should not be None"
        assert (
            state.get(f"{namespace}/status") == "success"
        ), "Action should have completed successfully"
        assert (
            state.get(f"{namespace}/image_id") == "ami-1234567890abcdef0"
        ), "Should track the shared image ID"

        # Verify shared accounts are tracked
        shared_accounts = state.get(f"{namespace}/shared_accounts")
        assert shared_accounts is not None, "Should track shared accounts"
        assert len(shared_accounts) == 2, "Should have shared with 2 accounts"
        assert "123456789012" in shared_accounts
        assert "098765432109" in shared_accounts

    except Exception as e:
        traceback.print_exc()
        pytest.fail(f"Test failed with exception: {e}")


def test_share_image_not_found(
    task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session
):
    """Test the share image action when AMI is not found."""
    try:
        # Create mock EC2 client
        mock_client = MagicMock()

        # Mock describe_images to return no images (image not found)
        mock_client.describe_images.return_value = {"Images": []}

        mock_session.client.return_value = mock_client

        save_actions(task_payload, deploy_spec.action_specs)
        save_state(task_payload, {})

        # Execute the handler
        event = task_payload.model_dump()
        response = execute_handler(event, None)

        # Parse the response
        updated_payload = TaskPayload(**response)
        assert (
            updated_payload.flow_control == "success"
        ), "Should complete successfully even when image not found"

        # Load state
        state = load_state(updated_payload)
        namespace = "share-image"

        # Verify behavior when image not found
        assert (
            state.get(f"{namespace}/status") == "skipped"
        ), "Should skip when image not found"
        assert f"{namespace}/error_message" in state, "Should have error message"
        assert "does not exist" in state.get(
            f"{namespace}/error_message", ""
        ), "Error message should mention image doesn't exist"

        # Verify modify_image_attribute was NOT called
        mock_client.modify_image_attribute.assert_not_called()

    except Exception as e:
        traceback.print_exc()
        pytest.fail(f"Test failed with exception: {e}")
