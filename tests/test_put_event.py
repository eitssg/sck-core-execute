import traceback
import pytest
from unittest.mock import patch, MagicMock

import core_framework as util
from core_framework.models import TaskPayload, DeploySpec

from core_execute.actionlib.actions.aws.put_event import (
    PutEventActionSpec,
    PutEventActionParams,
)

from core_execute.execute import save_state, save_actions, load_state
from core_execute.handler import handler as execute_handler


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
    Fixture to provide a deployspec data for testing.
    This can be used to mock the deployspec in tests.
    """
    params = {
        "Type": "STATUS",
        "Status": "SUCCESS",
        "Message": "Deployment completed successfully",
        "Identity": "prn:my-portfolio:my-app",
    }

    # validate the params here before we run the action
    validated_params = PutEventActionParams(**params)

    # Define the action specification
    action_spec = PutEventActionSpec(
        Name="event-namespace:action/test-put-event",
        Kind="AWS::PutEvent",
        Spec=validated_params.model_dump(),
        Scope="build",
    )

    return DeploySpec(Actions=[action_spec])


def test_put_event_action_success(task_payload: TaskPayload, deploy_spec: DeploySpec):
    """Test the put event action successful execution."""

    try:
        # Mock the EventActions.create method
        with patch("core_db.event.actions.EventActions.create") as mock_create:
            mock_event = {
                "id": "test-event-123",
                "status": "success",
                "message": "Event recorded successfully",
            }
            mock_create.return_value = mock_event

            save_actions(task_payload, deploy_spec.actions)
            save_state(task_payload, {})

            event = task_payload.model_dump()
            response = execute_handler(event, None)

            # Verify the response
            assert response is not None, "Response should not be None"
            assert isinstance(response, dict), "Response should be a dictionary"

            # Parse the response back into TaskPayload and check state
            updated_payload = TaskPayload(**response)

            # Load the saved state to verify completion
            state = load_state(updated_payload)
            print(f"\n=== PUT EVENT SUCCESS STATE ===")
            print("State:")
            print(util.to_yaml(state))

            # Check that state contains expected values
            if "actions" in state and len(state["actions"]) > 0:
                action_state = state["actions"][0].get("state", {})
                action_outputs = state["actions"][0].get("outputs", {})

                print(f"Action State: {action_state}")
                print("Action Outputs:")
                print(util.to_yaml(action_outputs))

                # Verify action completed successfully (no error state set)
                assert action_state.get("status") != "error"
                assert action_state.get("error_message") is None

                # Verify EventActions.create was called with correct parameters
                mock_create.assert_called_once_with(
                    "prn:my-portfolio:my-app",
                    event_type="STATUS",
                    item_type="portfolio",
                    status="SUCCESS",
                    message="Deployment completed successfully",
                )

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
        pytest.fail(f"Test failed due to exception: {e}")


def test_put_event_action_database_error(
    task_payload: TaskPayload, deploy_spec: DeploySpec
):
    """Test the put event action when database operation fails."""

    try:
        # Mock the EventActions.create method to raise an exception
        with patch("core_db.event.actions.EventActions.create") as mock_create:
            mock_create.side_effect = Exception("Database connection failed")

            save_actions(task_payload, deploy_spec.actions)
            save_state(task_payload, {})

            event = task_payload.model_dump()
            response = execute_handler(event, None)

            # Verify the response
            assert response is not None, "Response should not be None"
            assert isinstance(response, dict), "Response should be a dictionary"

            # Parse the response back into TaskPayload and check state
            updated_payload = TaskPayload(**response)

            # Load the saved state to verify error handling
            state = load_state(updated_payload)
            print(f"\n=== PUT EVENT DATABASE ERROR STATE ===")
            print("State:")
            print(util.to_yaml(state))

            # Check that state contains expected error values
            if "actions" in state and len(state["actions"]) > 0:
                action_state = state["actions"][0].get("state", {})
                action_outputs = state["actions"][0].get("outputs", {})

                print(f"Action State: {action_state}")
                print("Action Outputs:")
                print(util.to_yaml(action_outputs))

                # Verify error state tracking
                assert action_state.get("status") == "error"
                assert action_state.get("error_message") == "Database connection failed"
                assert action_state.get("event_type") == "STATUS"
                assert action_state.get("event_status") == "SUCCESS"
                assert (
                    action_state.get("event_message")
                    == "Deployment completed successfully"
                )
                assert action_state.get("event_identity") == "prn:my-portfolio:my-app"
                assert action_state.get("error_time") is not None

                # Verify error outputs
                assert action_outputs.get("status") == "error"
                assert (
                    action_outputs.get("error_message") == "Database connection failed"
                )
                assert "Failed to save event to database" in action_outputs.get(
                    "message", ""
                )
                assert action_outputs.get("error_time") is not None

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
        pytest.fail(f"Test failed due to exception: {e}")


def test_put_event_action_invalid_type(task_payload: TaskPayload):
    """Test the put event action with invalid event type."""

    try:
        # Create deploy spec with invalid event type
        params = {
            "Type": "INVALID_TYPE",
            "Status": "SUCCESS",
            "Message": "Test message",
            "Identity": "prn:my-portfolio:my-app",
        }

        action_spec = PutEventActionSpec(
            **{
                "name": "test-put-event-invalid",
                "kind": "AWS::PutEvent",
                "params": params,
                "scope": "build",
            }
        )
        deploy_spec = DeploySpec(**{"actions": [action_spec]})

        # Mock the EventActions.create method (shouldn't be called for real.  No DynamoDB is running)
        with patch("core_db.event.actions.EventActions.create") as mock_create:

            save_actions(task_payload, deploy_spec.actions)
            save_state(task_payload, {})

            event = task_payload.model_dump()
            response = execute_handler(event, None)

            # Verify the response
            assert response is not None, "Response should not be None"
            assert isinstance(response, dict), "Response should be a dictionary"

            # Parse the response back into TaskPayload and check state
            updated_payload = TaskPayload(**response)

            # Load the saved state to verify error handling
            state = load_state(updated_payload)
            print(f"\n=== PUT EVENT INVALID TYPE STATE ===")
            print("State:")
            print(util.to_yaml(state))

            # Check that state contains expected error values
            if "actions" in state and len(state["actions"]) > 0:
                action_state = state["actions"][0].get("state", {})
                action_outputs = state["actions"][0].get("outputs", {})

                print(f"Action State: {action_state}")
                print("Action Outputs:")
                print(util.to_yaml(action_outputs))

                # Verify error state tracking for invalid type
                assert action_state.get("status") == "error"
                assert "Invalid event type" in action_state.get("error_message", "")
                assert action_state.get("event_type") == "INVALID_TYPE"

                # Verify error outputs
                assert action_outputs.get("status") == "error"
                assert "Invalid event type" in action_outputs.get("error_message", "")

                # Verify EventActions.create was NOT called due to validation error
                mock_create.assert_not_called()

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
        pytest.fail(f"Test failed due to exception: {e}")


def test_put_event_action_different_types(task_payload: TaskPayload):
    """Test the put event action with different event types."""

    event_types = ["STATUS", "DEBUG", "INFO", "WARN", "ERROR"]

    for event_type in event_types:
        try:
            # Create deploy spec with different event type
            params = {
                "Type": event_type,
                "Status": f"TEST_{event_type}",
                "Message": f"Test {event_type.lower()} message",
                "Identity": "prn:my-portfolio:my-app",
            }

            action_spec = PutEventActionSpec(
                **{
                    "name": f"test-put-event-{event_type.lower()}",
                    "kind": "AWS::PutEvent",
                    "params": params,
                    "scope": "build",
                }
            )
            deploy_spec = DeploySpec(**{"actions": [action_spec]})

            # Mock the EventActions.create method
            with patch("core_db.event.actions.EventActions.create") as mock_create:
                mock_event = {"id": f"test-event-{event_type}", "status": "success"}
                mock_create.return_value = mock_event

                save_actions(task_payload, deploy_spec.actions)
                save_state(task_payload, {})

                event = task_payload.model_dump()
                response = execute_handler(event, None)

                # Verify the response
                assert (
                    response is not None
                ), f"Response should not be None for {event_type}"
                assert isinstance(
                    response, dict
                ), f"Response should be a dictionary for {event_type}"

                # Verify EventActions.create was called with correct parameters
                mock_create.assert_called_once_with(
                    "prn:my-portfolio:my-app",
                    event_type=event_type,
                    item_type="portfolio",
                    status=f"TEST_{event_type}",
                    message=f"Test {event_type.lower()} message",
                )

                print(f"✓ Event type {event_type} processed successfully")

        except Exception as e:
            print(f"An error occurred testing {event_type}: {e}")
            traceback.print_exc()
            pytest.fail(
                f"Test failed for event type {event_type} due to exception: {e}"
            )
