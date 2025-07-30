import traceback
import pytest
from unittest.mock import MagicMock

import core_framework as util
from core_framework.models import TaskPayload, DeploySpec

from core_execute.actionlib.actions.aws.empty_bucket import (
    EmptyBucketActionSpec,
    EmptyBucketActionParams,
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
            "Scope": "portfolio",
            "DataCenter": "zone-1",
        },
    }
    return TaskPayload(**data)


@pytest.fixture
def deploy_spec():
    """
    Fixture to provide a sample deploy spec for empty bucket testing.
    """
    params = {
        "Account": "123456789012",
        "Region": "us-east-1",
        "BucketName": "test-bucket-name",
    }
    action_spec = EmptyBucketActionSpec(
        **{
            "name": "test-empty-bucket",
            "kind": "AWS::EmptyBucket",
            "params": params,
            "scope": "build",
        }
    )
    return DeploySpec(**{"actions": [action_spec]})


def test_empty_bucket_action(
    task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session
):
    """Test the empty bucket action execution with comprehensive state tracking."""

    try:
        # Mock S3 resource and bucket hierarchy
        mock_bucket = MagicMock()

        # Configure the complete mock chain
        mock_s3_resource = MagicMock()
        mock_s3_resource.Bucket.return_value = mock_bucket

        mock_object_versions = MagicMock()
        mock_bucket.object_versions = mock_object_versions

        mock_limited_versions = MagicMock()
        mock_object_versions.limit.return_value = mock_limited_versions

        # FIRST ITERATION: Bucket has objects to delete
        mock_limited_versions.delete.return_value = [
            {
                "Deleted": [
                    {"Key": "file1.txt", "VersionId": "version1"},
                    {"Key": "file2.txt", "VersionId": "version2"},
                    {"Key": "file3.txt", "VersionId": "version3"},
                    {"Key": "file4.txt", "VersionId": "version4"},
                    {"Key": "file5.txt", "VersionId": "version5"},
                ]
            },
            {
                "Deleted": [
                    {"Key": "file6.txt", "VersionId": "version6"},
                    {"Key": "file7.txt", "VersionId": "version7"},
                ]
            },
        ]

        mock_session.resource.return_value = mock_s3_resource

        save_actions(task_payload, deploy_spec.actions)
        save_state(task_payload, {})

        event = task_payload.model_dump()
        response = execute_handler(event, None)

        # Verify the response
        assert response is not None, "Response should not be None"
        assert isinstance(response, dict), "Response should be a dictionary"

        # Print actual calls for debugging
        print(f"\nMock session resource calls: {mock_session.resource.call_args_list}")
        print(
            f"Mock S3 resource Bucket calls: {mock_s3_resource.Bucket.call_args_list}"
        )

        # Check bucket was accessed
        mock_s3_resource.Bucket.assert_called_with("test-bucket-name")
        mock_object_versions.limit.assert_called_with(count=5000)
        mock_limited_versions.delete.assert_called()

        # Parse the response back into TaskPayload and check state
        updated_payload = TaskPayload(**response)

        # Load the saved state to verify progress tracking
        state = load_state(updated_payload)
        print(f"\n=== FIRST ITERATION STATE ===")
        print(f"State:")
        print(util.to_yaml(state))

        # Check that state contains expected values
        if "actions" in state and len(state["actions"]) > 0:
            action_state = state["actions"][0].get("state", {})
            print(f"Action State: {action_state}")

            # Verify state tracking
            assert action_state.get("bucket_name") == "test-bucket-name"
            assert action_state.get("total_objects_deleted") == 7  # 5 + 2 objects
            assert action_state.get("batch_count") == 1

            # Check outputs
            action_outputs = state["actions"][0].get("outputs", {})
            print(f"Action Outputs: {action_outputs}")

            assert action_outputs.get("status") == "in_progress"
            assert action_outputs.get("total_objects_deleted") == 7
            assert action_outputs.get("current_batch") == 1
            assert action_outputs.get("last_batch_deleted") == 7

        # SECOND ITERATION: Bucket is now empty
        mock_limited_versions.delete.return_value = []  # No more objects to delete

        event = updated_payload.model_dump()
        response = execute_handler(event, None)

        # Verify final response
        assert response is not None, "Final response should not be None"
        final_payload = TaskPayload(**response)

        # Load the final state
        final_state = load_state(final_payload)
        print(f"\n=== FINAL ITERATION STATE ===")
        print(f"Final State:")
        print(util.to_yaml(final_state))

        if "actions" in final_state and len(final_state["actions"]) > 0:
            final_action_state = final_state["actions"][0].get("state", {})
            print(f"Final Action State: {final_action_state}")

            # Verify completion state
            assert final_action_state.get("status") == "completed"

            # Check final outputs
            final_action_outputs = final_state["actions"][0].get("outputs", {})
            print(f"Final Action Outputs: {final_action_outputs}")

            assert final_action_outputs.get("status") == "success"
            assert final_action_outputs.get("total_objects_deleted") == 7
            assert final_action_outputs.get("total_batches") == 1
            assert "Bucket 'test-bucket-name' is now empty" in final_action_outputs.get(
                "message", ""
            )

    except Exception as e:
        traceback.print_exc()
        pytest.fail(f"Test failed with exception: {e}")


def test_empty_bucket_action_bucket_not_exists(
    task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session
):
    """Test the empty bucket action when bucket doesn't exist."""

    try:
        # Mock S3 resource and bucket
        mock_s3_resource = MagicMock()
        mock_bucket = MagicMock()

        # Configure the mock chain
        mock_session.resource.return_value = mock_s3_resource
        mock_s3_resource.Bucket.return_value = mock_bucket

        # Mock ClientError for non-existent bucket
        from botocore.exceptions import ClientError

        error_response = {
            "Error": {
                "Code": "NoSuchBucket",
                "Message": "The specified bucket does not exist",
            }
        }
        mock_bucket.object_versions.limit.side_effect = ClientError(
            error_response, "ListObjectVersions"
        )

        save_actions(task_payload, deploy_spec.actions)
        save_state(task_payload, {})

        event = task_payload.model_dump()
        response = execute_handler(event, None)

        # Verify the response handles the non-existent bucket gracefully
        assert response is not None, "Response should not be None"
        assert isinstance(response, dict), "Response should be a dictionary"

        # Parse response and check state
        response_payload = TaskPayload(**response)
        state = load_state(response_payload)

        print(f"\n=== BUCKET NOT EXISTS STATE ===")
        print(f"State: {state}")

        if "actions" in state and len(state["actions"]) > 0:
            action_state = state["actions"][0].get("state", {})
            action_outputs = state["actions"][0].get("outputs", {})

            print(f"Action State: {action_state}")
            print(f"Action Outputs: {action_outputs}")

            # Verify error handling state
            assert action_state.get("status") == "completed_not_found"

            # Verify outputs for non-existent bucket
            assert action_outputs.get("status") == "success"
            assert action_outputs.get("total_objects_deleted") == 0
            assert action_outputs.get("total_batches") == 0
            assert "does not exist" in action_outputs.get("message", "")

        # Verify S3 operations were attempted
        mock_session.resource.assert_called()
        mock_s3_resource.Bucket.assert_called_with("test-bucket-name")

    except Exception as e:
        traceback.print_exc()
        pytest.fail(f"Test failed with exception: {e}")


def test_empty_bucket_action_multiple_batches(
    task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session
):
    """Test the empty bucket action with multiple batches."""

    try:
        # Mock S3 resource and bucket
        mock_s3_resource = MagicMock()
        mock_bucket = MagicMock()
        mock_object_versions = MagicMock()
        mock_limited_versions = MagicMock()

        # Configure the mock chain
        mock_session.resource.return_value = mock_s3_resource
        mock_s3_resource.Bucket.return_value = mock_bucket
        mock_bucket.object_versions = mock_object_versions
        mock_object_versions.limit.return_value = mock_limited_versions

        # Setup mock responses for multiple iterations
        delete_responses = [
            # First batch - 3 objects
            [
                {
                    "Deleted": [
                        {"Key": f"file{i}.txt", "VersionId": f"v{i}"}
                        for i in range(1, 4)
                    ]
                }
            ],
            # Second batch - 2 objects
            [
                {
                    "Deleted": [
                        {"Key": f"file{i}.txt", "VersionId": f"v{i}"}
                        for i in range(4, 6)
                    ]
                }
            ],
            # Third batch - empty (bucket is now empty)
            [],
        ]

        save_actions(task_payload, deploy_spec.actions)
        save_state(task_payload, {})

        current_payload = task_payload
        for batch_num, delete_response in enumerate(delete_responses, 1):
            print(f"\n=== BATCH {batch_num} ===")

            mock_limited_versions.delete.return_value = delete_response

            event = current_payload.model_dump()
            response = execute_handler(event, None)

            assert (
                response is not None
            ), f"Response should not be None for batch {batch_num}"
            current_payload = TaskPayload(**response)

            # Check state after each batch
            state = load_state(current_payload)
            if "actions" in state and len(state["actions"]) > 0:
                action_state = state["actions"][0].get("state", {})
                action_outputs = state["actions"][0].get("outputs", {})

                print(f"Batch {batch_num} State: {action_state}")
                print(f"Batch {batch_num} Outputs: {action_outputs}")

                if batch_num < 3:  # In progress batches
                    expected_total = sum(
                        len(resp[0]["Deleted"])
                        for resp in delete_responses[:batch_num]
                        if resp
                    )
                    assert action_state.get("total_objects_deleted") == expected_total
                    assert action_state.get("batch_count") == batch_num
                    assert action_outputs.get("status") == "in_progress"
                else:  # Final batch - completed
                    assert action_state.get("status") == "completed"
                    assert action_outputs.get("status") == "success"
                    assert action_outputs.get("total_objects_deleted") == 5  # 3 + 2
                    assert action_outputs.get("total_batches") == 2

    except Exception as e:
        traceback.print_exc()
        pytest.fail(f"Test failed with exception: {e}")
