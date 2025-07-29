import traceback
import pytest
from unittest.mock import MagicMock

from core_framework.models import TaskPayload, DeploySpec

from core_execute.actionlib.actions.aws.delete_stack import (
    DeleteStackActionSpec,
    DeleteStackActionParams,
)

from core_execute.execute import save_state, save_actions, load_state

from core_execute.handler import handler as execute_handler

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
    params = {
        "Account": "test-db-account",
        "Region": "us-east-1",
        "StackName": "test-stack-name",
        "SuccessStatuses": ["DELETE_COMPLETE"],
    }
    delete_stack_action = DeleteStackActionSpec(**{"params": params})
    return DeploySpec(**{"actions": [delete_stack_action]})


def test_delete_stack_action(task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session):

    try:
        # FIRST ITERATION: Stack exists and needs to be deleted
        mock_client = MagicMock()

        # Use a custom function to handle unlimited calls
        def mock_describe_stacks(*args, **kwargs):
            # First call returns CREATE_COMPLETE, all subsequent calls return DELETE_IN_PROGRESS
            if not hasattr(mock_describe_stacks, "call_count"):
                mock_describe_stacks.call_count = 0
            mock_describe_stacks.call_count += 1

            if mock_describe_stacks.call_count == 1:
                # First call: Stack ready for deletion
                return {
                    "Stacks": [
                        {
                            "StackName": "test-stack-name",
                            "StackId": "arn:aws:cloudformation:us-east-1:123456789012:stack/test-stack-name/12345678-1234-1234-1234-123456789012",
                            "StackStatus": "CREATE_COMPLETE",
                            "CreationTime": "2023-10-01T12:00:00Z",
                            "LastUpdatedTime": "2023-10-01T12:00:00Z",
                            "Description": "Test stack for deletion",
                        }
                    ]
                }
            else:
                # All subsequent calls: Stack deletion in progress
                return {
                    "Stacks": [
                        {
                            "StackName": "test-stack-name",
                            "StackId": "arn:aws:cloudformation:us-east-1:123456789012:stack/test-stack-name/12345678-1234-1234-1234-123456789012",
                            "StackStatus": "DELETE_IN_PROGRESS",
                            "CreationTime": "2023-10-01T12:00:00Z",
                            "LastUpdatedTime": "2023-10-01T12:30:00Z",
                            "Description": "Test stack for deletion",
                        }
                    ]
                }

        mock_client.describe_stacks.side_effect = mock_describe_stacks

        # Mock successful delete_stack operation
        mock_client.delete_stack.return_value = {
            "ResponseMetadata": {
                "RequestId": "12345678-1234-1234-1234-123456789012",
                "HTTPStatusCode": 200,
            }
        }

        # Mock stack events and resources (for tracking)
        mock_client.describe_stack_events.return_value = {"StackEvents": []}
        mock_client.list_stack_resources.return_value = {"StackResourceSummaries": []}

        mock_session.client.return_value = mock_client

        save_actions(task_payload, deploy_spec.actions)
        save_state(task_payload, {})

        # FIRST ITERATION: Call execute_handler - should initiate deletion and continue executing
        print("🔄 First iteration: Initiating stack deletion...")
        event = task_payload.model_dump()
        response = execute_handler(event, None)

        # Check if the response is as expected
        assert response is not None, "Response should not be None"
        assert isinstance(response, dict), "Response should be a dictionary"

        # Parse the response back into TaskPayload
        task_payload = TaskPayload(**response)

        # Should be "execute" after initiating deletion (to continue checking status)
        assert task_payload.flow_control == "execute", f"Expected flow_control to be 'execute', got '{task_payload.flow_control}'"

        # Verify delete_stack was called
        mock_client.delete_stack.assert_called_once()

        print(f"✅ First iteration completed with flow_control: {task_payload.flow_control}")

        # SECOND ITERATION: Stack deletion completed
        print("🔄 Second iteration: Stack deletion completed...")
        mock_client = MagicMock()

        # Mock stack with DELETE_COMPLETE status
        mock_client.describe_stacks.return_value = {
            "Stacks": [
                {
                    "StackName": "test-stack-name",
                    "StackId": "arn:aws:cloudformation:us-east-1:123456789012:stack/test-stack-name/12345678-1234-1234-1234-123456789012",
                    "StackStatus": "DELETE_COMPLETE",
                    "CreationTime": "2023-10-01T12:00:00Z",
                    "LastUpdatedTime": "2023-10-01T12:30:00Z",
                }
            ]
        }

        mock_client.describe_stack_events.return_value = {"StackEvents": []}
        mock_client.list_stack_resources.return_value = {"StackResourceSummaries": []}

        mock_session.client.return_value = mock_client

        # Call execute_handler again with updated mock
        event = task_payload.model_dump()
        response = execute_handler(event, None)

        # Check if the response is as expected
        assert response is not None, "Response should not be None"
        assert isinstance(response, dict), "Response should be a dictionary"

        # Parse the response back into TaskPayload
        task_payload = TaskPayload(**response)

        # Should be "success" after finding DELETE_COMPLETE status
        assert task_payload.flow_control == "success", f"Expected flow_control to be 'success', got '{task_payload.flow_control}'"

        state = load_state(task_payload)
        assert state is not None, "State should not be None"
        assert isinstance(state, dict), "State should be a dictionary"

        action_name = "action-aws-deletestack-name"
        assert state[f"{action_name}/DeletionCompleted"] is True
        assert state[f"{action_name}/DeletionResult"] == "SUCCESS"

        print(f"✅ Second iteration completed with flow_control: {task_payload.flow_control}")
        print("✅ All stack deletion test iterations passed successfully!")

    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        print(traceback.format_exc())
        assert False, str(e)


def test_lambda_handler_delete_in_progress(task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session):
    """Test scenario where stack deletion is in progress"""

    try:
        # Mock stack with DELETE_IN_PROGRESS status - use unlimited calls
        mock_client = MagicMock()

        def mock_describe_stacks_in_progress(*args, **kwargs):
            return {
                "Stacks": [
                    {
                        "StackName": "test-stack-name",
                        "StackId": "arn:aws:cloudformation:us-east-1:123456789012:stack/test-stack-name/12345678-1234-1234-1234-123456789012",
                        "StackStatus": "DELETE_IN_PROGRESS",
                        "CreationTime": "2023-10-01T12:00:00Z",
                    }
                ]
            }

        mock_client.describe_stacks.side_effect = mock_describe_stacks_in_progress
        mock_client.describe_stack_events.return_value = {"StackEvents": []}
        mock_client.list_stack_resources.return_value = {"StackResourceSummaries": []}

        mock_session.client.return_value = mock_client

        save_actions(task_payload, deploy_spec.actions)
        save_state(task_payload, {})

        event = task_payload.model_dump()
        response = execute_handler(event, None)
        task_payload = TaskPayload(**response)

        # Should be "execute" when deletion is in progress (to continue checking)
        assert task_payload.flow_control == "execute", f"Expected flow_control to be 'execute', got '{task_payload.flow_control}'"

        # delete_stack should NOT be called since deletion is already in progress
        mock_client.delete_stack.assert_not_called()

        print("✅ Delete in progress test passed")

    except Exception as e:
        print(f"❌ Test failed: {e}")
        assert False, str(e)


def test_lambda_handler_delete_failed(task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session):
    """Test scenario where stack deletion fails"""

    try:
        # Mock stack with DELETE_FAILED status - use unlimited calls
        mock_client = MagicMock()

        def mock_describe_stacks_failed(*args, **kwargs):
            return {
                "Stacks": [
                    {
                        "StackName": "test-stack-name",
                        "StackId": "arn:aws:cloudformation:us-east-1:123456789012:stack/test-stack-name/12345678-1234-1234-1234-123456789012",
                        "StackStatus": "DELETE_FAILED",
                        "CreationTime": "2023-10-01T12:00:00Z",
                    }
                ]
            }

        mock_client.describe_stacks.side_effect = mock_describe_stacks_failed

        # Mock failed resources
        mock_client.list_stack_resources.return_value = {
            "StackResourceSummaries": [
                {
                    "LogicalResourceId": "MyS3Bucket",
                    "PhysicalResourceId": "my-bucket-12345",
                    "ResourceType": "AWS::S3::Bucket",
                    "ResourceStatus": "DELETE_FAILED",
                    "ResourceStatusReason": "The bucket you tried to delete is not empty",
                }
            ]
        }

        mock_client.describe_stack_events.return_value = {"StackEvents": []}

        mock_session.client.return_value = mock_client

        save_actions(task_payload, deploy_spec.actions)
        save_state(task_payload, {})

        event = task_payload.model_dump()
        response = execute_handler(event, None)
        task_payload = TaskPayload(**response)

        # Should be "failure" when deletion fails
        assert task_payload.flow_control == "failure", f"Expected flow_control to be 'failure', got '{task_payload.flow_control}'"

        state = load_state(task_payload)
        action_name = "action-aws-deletestack-name"

        assert state[f"{action_name}/DeletionResult"] == "FAILED"
        assert f"{action_name}/FailedResources" in state

        failed_resources = state[f"{action_name}/FailedResources"]
        assert len(failed_resources) == 1
        assert failed_resources[0]["LogicalResourceId"] == "MyS3Bucket"

        print("✅ Delete failed test passed")

    except Exception as e:
        print(f"❌ Test failed: {e}")
        assert False, str(e)
