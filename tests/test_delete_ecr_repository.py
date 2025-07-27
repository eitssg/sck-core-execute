from typing import Any
import traceback
from unittest import mock
import pytest
from unittest.mock import MagicMock
from datetime import datetime, timezone
from botocore.exceptions import ClientError

from core_framework.models import TaskPayload, DeploySpec

from core_execute.actionlib.actions.aws.delete_ecr_repository import (
    DeleteEcrRepositoryActionSpec,
    DeleteEcrRepositoryActionParams,
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
    Parameters are fore: DeleteEcrRepositoryActionParams
    """
    spec: dict[str, Any] = {
        "Params": {
            "Account": "154798051514",
            "Region": "ap-southeast-1",
            "RepositoryName": "my-ecr-repository",
        }
    }

    action_spec = DeleteEcrRepositoryActionSpec(**spec)

    deploy_spec: dict[str, Any] = {"actions": [action_spec]}

    return DeploySpec(**deploy_spec)


def test_delete_ecr_repository_action(
    task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session
):

    try:

        creation_time = datetime(2023, 10, 1, 12, 0, 0, tzinfo=timezone.utc)

        mock_client = MagicMock()

        # Mock describe_repositories - returns repository info before deletion
        mock_client.describe_repositories.return_value = {
            "repositories": [
                {
                    "repositoryArn": "arn:aws:ecr:ap-southeast-1:154798051514:repository/my-ecr-repository",
                    "registryId": "154798051514",
                    "repositoryName": "my-ecr-repository",
                    "repositoryUri": "154798051514.dkr.ecr.ap-southeast-1.amazonaws.com/my-ecr-repository",
                    "createdAt": creation_time,
                    "imageTagMutability": "MUTABLE",
                    "imageScanningConfiguration": {"scanOnPush": False},
                    "encryptionConfiguration": {"encryptionType": "AES256"},
                    "repositorySizeInBytes": 1024000,  # 1MB
                    "imageCount": 3,
                }
            ]
        }

        # Mock delete_repository - returns successful deletion
        mock_client.delete_repository.return_value = {
            "repository": {
                "repositoryArn": "arn:aws:ecr:ap-southeast-1:154798051514:repository/my-ecr-repository",
                "registryId": "154798051514",
                "repositoryName": "my-ecr-repository",
                "repositoryUri": "154798051514.dkr.ecr.ap-southeast-1.amazonaws.com/my-ecr-repository",
                "createdAt": creation_time,
                "imageTagMutability": "MUTABLE",
                "repositorySizeInBytes": 1024000,
                "imageCount": 3,
            }
        }

        mock_session.client.return_value = mock_client

        save_actions(task_payload, deploy_spec.actions)
        save_state(task_payload, {})

        # Execute the action
        event = task_payload.model_dump()
        result = execute_handler(event, None)

        assert result is not None, "Result should not be None"
        assert isinstance(result, dict), "Result should be a dictionary"

        task_payload = TaskPayload(**result)

        # Validate the flow control in the task payload
        assert (
            task_payload.flow_control == "success"
        ), f"Expected flow_control to be 'success', got '{task_payload.flow_control}'"

        state = load_state(task_payload)

        # Verify ECR client method calls
        mock_client.describe_repositories.assert_called_once_with(
            registryId="154798051514", repositoryNames=["my-ecr-repository"]
        )

        mock_client.delete_repository.assert_called_once_with(
            registryId="154798051514", repositoryName="my-ecr-repository", force=True
        )

        # Validate state outputs that should be set by the action
        action_name = "action-aws-deleteecrrepository-name"

        # Check basic parameters are stored in state
        assert f"{action_name}/RepositoryName" in state
        assert state[f"{action_name}/RepositoryName"] == "my-ecr-repository"

        assert f"{action_name}/Region" in state
        assert state[f"{action_name}/Region"] == "ap-southeast-1"

        assert f"{action_name}/Account" in state
        assert state[f"{action_name}/Account"] == "154798051514"

        # Check deletion operation tracking
        assert f"{action_name}/DeletionStarted" in state
        assert state[f"{action_name}/DeletionStarted"] is True

        assert f"{action_name}/DeletionCompleted" in state
        assert state[f"{action_name}/DeletionCompleted"] is True

        assert f"{action_name}/DeletionResult" in state
        assert state[f"{action_name}/DeletionResult"] == "SUCCESS"

        assert f"{action_name}/RepositoryExisted" in state
        assert state[f"{action_name}/RepositoryExisted"] is True

        # Check repository metadata captured before deletion
        assert f"{action_name}/RepositoryUri" in state
        assert (
            state[f"{action_name}/RepositoryUri"]
            == "154798051514.dkr.ecr.ap-southeast-1.amazonaws.com/my-ecr-repository"
        )

        assert f"{action_name}/ImageCount" in state
        assert state[f"{action_name}/ImageCount"] == 3

        assert f"{action_name}/RepositorySize" in state
        assert state[f"{action_name}/RepositorySize"] == 1024000

        assert f"{action_name}/CreatedAt" in state
        assert creation_time == state[f"{action_name}/CreatedAt"]

        # Check timing information
        assert f"{action_name}/StartTime" in state
        assert f"{action_name}/CompletionTime" in state

        # Check status
        assert f"{action_name}/StatusCode" in state
        assert state[f"{action_name}/StatusCode"] == "complete"

        print("✅ All ECR repository deletion validations passed")
        print(f"📊 Repository: {state.get(f'{action_name}/RepositoryName')}")
        print(f"📊 Deletion Result: {state.get(f'{action_name}/DeletionResult')}")
        print(f"📊 Images Deleted: {state.get(f'{action_name}/ImageCount')}")
        print(f"📊 Size Deleted: {state.get(f'{action_name}/RepositorySize')} bytes")

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
        pytest.fail(f"Test failed due to an exception: {e}")


def test_delete_ecr_repository_not_found(
    task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session
):
    """Test deletion of a repository that doesn't exist."""

    try:
        mock_client = MagicMock()

        # Mock describe_repositories - repository not found
        mock_client.describe_repositories.side_effect = ClientError(
            error_response={
                "Error": {
                    "Code": "RepositoryNotFoundException",
                    "Message": "The repository with name 'my-ecr-repository' does not exist in the registry with id '154798051514'",
                }
            },
            operation_name="DescribeRepositories",
        )

        mock_session.client.return_value = mock_client

        save_actions(task_payload, deploy_spec.actions)
        save_state(task_payload, {})

        event = task_payload.model_dump()
        result = execute_handler(event, None)

        assert result is not None, "Result should not be None"
        task_payload = TaskPayload(**result)

        # Should still succeed when repository doesn't exist
        assert (
            task_payload.flow_control == "success"
        ), f"Expected flow_control to be 'success', got '{task_payload.flow_control}'"

        state = load_state(task_payload)

        # Verify that describe was called but delete was not
        mock_client.describe_repositories.assert_called_once()
        mock_client.delete_repository.assert_not_called()

        action_name = "action-aws-deleteecrrepository-name"

        # Check that repository was marked as not existing
        assert f"{action_name}/RepositoryExisted" in state
        assert state[f"{action_name}/RepositoryExisted"] is False

        assert f"{action_name}/DeletionResult" in state
        assert state[f"{action_name}/DeletionResult"] == "NOT_FOUND"

        assert f"{action_name}/DeletionCompleted" in state
        assert state[f"{action_name}/DeletionCompleted"] is True

        print("✅ Repository not found test passed")

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
        pytest.fail(f"Test failed due to an exception: {e}")


def test_delete_ecr_repository_deletion_error(
    task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session
):
    """Test deletion failure scenario."""

    try:
        creation_time = datetime(2023, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_client = MagicMock()

        # Mock describe_repositories - repository exists
        mock_client.describe_repositories.return_value = {
            "repositories": [
                {
                    "repositoryArn": "arn:aws:ecr:ap-southeast-1:154798051514:repository/my-ecr-repository",
                    "registryId": "154798051514",
                    "repositoryName": "my-ecr-repository",
                    "repositoryUri": "154798051514.dkr.ecr.ap-southeast-1.amazonaws.com/my-ecr-repository",
                    "createdAt": creation_time,
                    "repositorySizeInBytes": 1024000,
                    "imageCount": 3,
                }
            ]
        }

        # Mock delete_repository - fails with access denied
        mock_client.delete_repository.side_effect = ClientError(
            error_response={
                "Error": {
                    "Code": "AccessDeniedException",
                    "Message": "User does not have permission to delete repository",
                }
            },
            operation_name="DeleteRepository",
        )

        mock_session.client.return_value = mock_client

        save_actions(task_payload, deploy_spec.actions)
        save_state(task_payload, {})

        event = task_payload.model_dump()
        result = execute_handler(event, None)

        assert result is not None, "Result should not be None"
        task_payload = TaskPayload(**result)

        # Should fail when deletion encounters an error
        assert (
            task_payload.flow_control == "failure"
        ), f"Expected flow_control to be 'failure', got '{task_payload.flow_control}'"

        state = load_state(task_payload)

        action_name = "action-aws-deleteecrrepository-name"

        # Check that repository was found but deletion failed
        assert f"{action_name}/RepositoryExisted" in state
        assert state[f"{action_name}/RepositoryExisted"] is True

        assert f"{action_name}/DeletionResult" in state
        assert state[f"{action_name}/DeletionResult"] == "FAILED"

        assert f"{action_name}/FailureReason" in state
        assert "AccessDeniedException" in state[f"{action_name}/FailureReason"]

        print("✅ Repository deletion error test passed")

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
        pytest.fail(f"Test failed due to an exception: {e}")
