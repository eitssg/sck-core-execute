import traceback
from webbrowser import get
import pytest
from unittest.mock import MagicMock

from botocore.exceptions import ClientError

import core_framework as util
from core_framework.models import TaskPayload, DeploySpec

from core_execute.actionlib.actions.aws.get_stack_references import (
    GetStackReferencesActionResource,
    GetStackReferencesActionSpec,
)

from core_execute.execute import save_state, save_actions, load_state
from core_execute.handler import handler as execute_handler

from .aws_fixtures import *

action_name = "test-get-stack-references"


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
            "Scope": "portfolio",
            "DataCenter": "zone-1",
        },
    }
    return TaskPayload.model_validate(data)


@pytest.fixture
def deploy_spec() -> DeploySpec:
    """
    Fixture to provide a sample deploy spec for get stack references testing.
    """
    params = {
        "Account": "123456789012",
        "Region": "us-east-1",
        "StackName": "test-stack-name",
    }
    spec = GetStackReferencesActionSpec.model_validate(params)
    action_resource = GetStackReferencesActionResource(name=action_name, spec=spec)
    return DeploySpec(actions=[action_resource])


def test_get_stack_references_action_with_references(task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session):
    """Test the get stack references action when export has references."""

    try:
        # Mock CloudFormation client
        mock_cfn_client = mock_session().client(
            "cloudformation",
            client_type="target",
            aws_account_id="123456789012",
            region_name="us-east-1",
            **get_role_credentials(RoleArn=util.get_provisioning_role_arn("123456789012")),
        )

        # Mock the list_imports response with sample importing stacks
        mock_list_imports_response = {"Imports": ["importing-stack-1", "importing-stack-2", "importing-stack-3"]}

        mock_cfn_client.list_imports.return_value = mock_list_imports_response

        save_actions(task_payload, deploy_spec.actions)
        save_state(task_payload, {})

        event = task_payload.model_dump()
        response = execute_handler(event, None)

        # Verify the response
        assert response is not None, "Response should not be None"
        assert isinstance(response, dict), "Response should be a dictionary"

        # Parse the response back into TaskPayload and check state
        updated_payload = TaskPayload.model_validate(response)

        # Load the saved state to verify progress tracking
        state = load_state(updated_payload)
        print(f"\n=== GET STACK REFERENCES WITH REFERENCES STATE ===")
        print("State:")
        print(util.to_yaml(state))

        # Check that state contains expected values
        if "actions" in state and len(state["actions"]) > 0:
            action_state = state["actions"][0].get("state", {})
            print(f"Action State: {action_state}")

            # Verify state tracking
            assert action_state.get("stack_name") == "test-stack-name"
            assert action_state.get("output_name") == "DefaultExport"  # default value
            assert action_state.get("export_name") == "test-stack-name:DefaultExport"
            assert action_state.get("account") == "123456789012"
            assert action_state.get("region") == "us-east-1"
            assert action_state.get("status") == "completed_with_references"
            assert action_state.get("num_references") == 3
            assert action_state.get("references") == [
                "importing-stack-1",
                "importing-stack-2",
                "importing-stack-3",
            ]
            assert action_state.get("start_time") is not None
            assert action_state.get("completion_time") is not None

            # Check outputs
            action_outputs = state["actions"][0].get("outputs", {})
            print("Action Outputs:")
            print(util.to_yaml(action_outputs))

            # Verify basic action outputs
            assert action_outputs.get("status") == "success"
            assert action_outputs.get("stack_name") == "test-stack-name"
            assert action_outputs.get("output_name") == "DefaultExport"
            assert action_outputs.get("export_name") == "test-stack-name:DefaultExport"
            assert action_outputs.get("account") == "123456789012"
            assert action_outputs.get("region") == "us-east-1"
            assert action_outputs.get("has_references") == True
            assert action_outputs.get("num_references") == 3
            assert action_outputs.get("references") == [
                "importing-stack-1",
                "importing-stack-2",
                "importing-stack-3",
            ]
            assert "is referenced by 3 stack(s)" in action_outputs.get("message", "")

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
        pytest.fail(f"Test failed due to exception: {e}")


def test_get_stack_references_action_export_not_found(task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session):
    """Test the get stack references action when export doesn't exist."""

    reset()

    try:
        # Mock CloudFormation client
        mock_cfn_client = mock_session().client(
            "cloudformation",
            client_type="target",
            aws_account_id="123456789012",
            region_name="us-east-1",
            **get_role_credentials(RoleArn=util.get_provisioning_role_arn("123456789012")),
        )

        # Mock ClientError for non-existent export
        from botocore.exceptions import ClientError

        error_response = {
            "Error": {
                "Code": "ValidationError",
                "Message": "Export test-stack-name:DefaultExport does not exist",
            }
        }
        mock_cfn_client.list_imports.side_effect = ClientError(error_response, "ListImports")

        save_actions(task_payload, deploy_spec.actions)
        save_state(task_payload, {})

        event = task_payload.model_dump()
        response = execute_handler(event, None)

        # Verify the response handles the non-existent export gracefully
        assert response is not None, "Response should not be None"
        assert isinstance(response, dict), "Response should be a dictionary"

        # Parse response and check state
        response_payload = TaskPayload.model_validate(response)
        state = load_state(response_payload)

        print(f"\n=== EXPORT NOT FOUND STATE ===")
        print("State:")
        print(util.to_yaml(state))

        if "actions" in state and len(state["actions"]) > 0:
            action_state = state["actions"][0].get("state", {})
            action_outputs = state["actions"][0].get("outputs", {})

            print(f"Action State: {action_state}")
            print(f"Action Outputs: {action_outputs}")

            # Verify error handling state
            assert action_state.get("status") == "completed_export_not_found"
            assert action_state.get("num_references") == 0
            assert action_state.get("references") == []

            # Verify outputs for non-existent export
            assert action_outputs.get("status") == "success"
            assert action_outputs.get("has_references") == False
            assert action_outputs.get("num_references") == 0
            assert action_outputs.get("references") == []
            assert "does not exist" in action_outputs.get("message", "")

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
        pytest.fail(f"Test failed due to exception: {e}")


def test_get_stack_references_action_no_references(task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session):
    """Test the get stack references action when export exists but has no references."""

    reset()

    try:
        # Mock CloudFormation client
        mock_cfn_client = mock_session().client(
            "cloudformation",
            client_type="target",
            aws_account_id="123456789012",
            region_name="us-east-1",
            **get_role_credentials(RoleArn=util.get_provisioning_role_arn("123456789012")),
        )

        error_response = {
            "Error": {
                "Code": "ValidationError",
                "Message": "Export test-stack-name:DefaultExport is not imported by any stack",
            }
        }
        mock_cfn_client.list_imports.side_effect = ClientError(error_response, "ListImports")

        save_actions(task_payload, deploy_spec.actions)
        save_state(task_payload, {})

        event = task_payload.model_dump()
        response = execute_handler(event, None)

        # Verify the response
        assert response is not None, "Response should not be None"
        assert isinstance(response, dict), "Response should be a dictionary"

        # Parse response and check state
        response_payload = TaskPayload.model_validate(response)
        state = load_state(response_payload)

        print(f"\n=== NO REFERENCES STATE ===")
        print("State:")
        print(util.to_yaml(state))

        if "actions" in state and len(state["actions"]) > 0:
            action_state = state["actions"][0].get("state", {})
            action_outputs = state["actions"][0].get("outputs", {})

            print(f"Action State: {action_state}")
            print(f"Action Outputs: {action_outputs}")

            # Verify completion state for export with no references
            assert action_state.get("status") == "completed_no_references"
            assert action_state.get("num_references") == 0
            assert action_state.get("references") == []

            # Verify outputs
            assert action_outputs.get("status") == "success"
            assert action_outputs.get("has_references") == False
            assert action_outputs.get("num_references") == 0
            assert action_outputs.get("references") == []
            assert "is not referenced by any stacks" in action_outputs.get("message", "")

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
        pytest.fail(f"Test failed due to exception: {e}")


def test_get_stack_references_action_custom_output_name(task_payload: TaskPayload, mock_session):
    """Test the get stack references action with custom output name."""

    reset()

    try:
        # Create deploy spec with custom output name
        params = {
            "Account": "123456789012",
            "Region": "us-east-1",
            "StackName": "test-stack-name",
            "OutputName": "CustomExport",
        }
        spec = GetStackReferencesActionSpec.model_validate(params)
        action_resource = GetStackReferencesActionResource(name="test-get-stack-references-custom", spec=spec)

        deploy_spec = DeploySpec(actions=[action_resource])

        # Mock CloudFormation client
        mock_cfn_client = mock_session().client(
            "cloudformation",
            client_type="target",
            aws_account_id="123456789012",
            region_name="us-east-1",
            **get_role_credentials(RoleArn=util.get_provisioning_role_arn("123456789012")),
        )

        # Mock the list_imports response with one importing stack
        mock_list_imports_response = {"Imports": ["importing-stack-custom"]}

        mock_cfn_client.list_imports.return_value = mock_list_imports_response

        save_actions(task_payload, deploy_spec.actions)
        save_state(task_payload, {})

        event = task_payload.model_dump()
        response = execute_handler(event, None)

        # Verify the response
        assert response is not None, "Response should not be None"
        assert isinstance(response, dict), "Response should be a dictionary"

        # Parse the response back into TaskPayload and check state
        updated_payload = TaskPayload.model_validate(response)

        # Load the saved state to verify progress tracking
        state = load_state(updated_payload)
        print(f"\n=== CUSTOM OUTPUT NAME STATE ===")
        print("State:")
        print(util.to_yaml(state))

        # Check that state contains expected values
        if "actions" in state and len(state["actions"]) > 0:
            action_state = state["actions"][0].get("state", {})
            action_outputs = state["actions"][0].get("outputs", {})

            print(f"Action State: {action_state}")
            print(f"Action Outputs: {action_outputs}")

            # Verify state tracking with custom output name
            assert action_state.get("output_name") == "CustomExport"
            assert action_state.get("export_name") == "test-stack-name:CustomExport"
            assert action_state.get("status") == "completed_with_references"
            assert action_state.get("num_references") == 1

            # Verify outputs
            assert action_outputs.get("output_name") == "CustomExport"
            assert action_outputs.get("export_name") == "test-stack-name:CustomExport"
            assert action_outputs.get("has_references") == True
            assert action_outputs.get("num_references") == 1
            assert action_outputs.get("references") == ["importing-stack-custom"]

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
        pytest.fail(f"Test failed due to exception: {e}")
