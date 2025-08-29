import traceback
import pytest
from unittest.mock import MagicMock

import core_framework as util

from core_framework.models import TaskPayload, DeploySpec

from core_execute.actionlib.actions.aws.unprotect_elb import (
    UnprotectELBActionSpec,
    UnprotectELBActionParams,
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
    validated_params = UnprotectELBActionParams(
        **{
            "Account": "123456789012",  # Fixed: 12 digits
            "Region": util.get_region(),
            "LoadBalancer": "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/my-load-balancer/1234567890abcdef",  # Fixed: Use ARN instead of name
        }
    )

    action_spec = UnprotectELBActionSpec(
        Name="unprotect-elb", Spec=validated_params.model_dump()
    )

    return DeploySpec(actions=[action_spec])


def test_unprotect_elb(
    task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session
):
    """Test the unprotect ELB action successful execution."""
    try:
        # Create mock ELBv2 client (not EC2)
        mock_client = MagicMock()

        # Mock describe_load_balancers for ELBv2 (different format than classic ELB)
        mock_client.describe_load_balancers.return_value = {
            "LoadBalancers": [
                {
                    "LoadBalancerArn": "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/my-load-balancer/1234567890abcdef",
                    "LoadBalancerName": "my-load-balancer",
                    "Scheme": "internet-facing",
                    "Type": "application",
                    "State": {"Code": "active"},
                    "AvailabilityZones": [
                        {"ZoneName": "us-east-1a", "SubnetId": "subnet-12345678"},
                        {"ZoneName": "us-east-1b", "SubnetId": "subnet-87654321"},
                    ],
                    "SecurityGroups": ["sg-12345678"],
                }
            ]
        }

        # Mock modify_load_balancer_attributes
        mock_client.modify_load_balancer_attributes.return_value = {
            "ResponseMetadata": {
                "RequestId": "mock-request-id-123",
                "HTTPStatusCode": 200,
            }
        }

        mock_session.client.return_value = mock_client

        save_actions(
            task_payload, deploy_spec.actions
        )  # Fixed: use .actions not .action_specs
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
        assert state is not None, "State should not be None"

        namespace = "unprotect-elb"

        # Verify ELBv2 API calls were made correctly
        mock_client.describe_load_balancers.assert_called_once_with(
            LoadBalancerArns=[
                "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/my-load-balancer/1234567890abcdef"
            ]
        )

        mock_client.modify_load_balancer_attributes.assert_called_once_with(
            LoadBalancerArn="arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/my-load-balancer/1234567890abcdef",
            Attributes=[{"Key": "deletion_protection.enabled", "Value": "false"}],
        )

        # Verify state tracking
        assert state.get(f"{namespace}/status") == "success", "Status should be success"
        assert (
            state.get(f"{namespace}/load_balancer_arn")
            == "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/my-load-balancer/1234567890abcdef"
        )
        assert (
            state.get(f"{namespace}/deletion_protection_disabled") == True
        ), "Should track that protection was disabled"
        assert (
            state.get(f"{namespace}/load_balancer_name") == "my-load-balancer"
        ), "Should capture load balancer name"
        assert (
            state.get(f"{namespace}/load_balancer_type") == "application"
        ), "Should capture load balancer type"
        assert (
            state.get(f"{namespace}/load_balancer_scheme") == "internet-facing"
        ), "Should capture load balancer scheme"
        assert (
            state.get(f"{namespace}/load_balancer_state") == "active"
        ), "Should capture load balancer state"

    except Exception as e:
        traceback.print_exc()
        pytest.fail(f"Test failed with exception: {e}")


def test_unprotect_elb_skip_none(task_payload: TaskPayload, mock_session):
    """Test the unprotect ELB action when LoadBalancer is 'none'."""
    try:
        # Create params with 'none' load balancer
        validated_params = UnprotectELBActionParams(
            **{
                "Account": "123456789012",
                "Region": util.get_region(),
                "LoadBalancer": "none",
            }
        )

        action_spec = UnprotectELBActionSpec(
            Name="unprotect-elb-skip", Spec=validated_params.model_dump()
        )

        deploy_spec = DeploySpec(actions=[action_spec])

        # Create mock client (shouldn't be called)
        mock_client = MagicMock()
        mock_session.client.return_value = mock_client

        save_actions(task_payload, deploy_spec.actions)
        save_state(task_payload, {})

        # Execute the handler
        event = task_payload.model_dump()
        response = execute_handler(event, None)

        # Parse the response
        updated_payload = TaskPayload(**response)
        assert (
            updated_payload.flow_control == "success"
        ), "Should complete successfully when skipping"

        # Load state
        state = load_state(updated_payload)
        namespace = "unprotect-elb-skip"

        # Verify skipped behavior
        assert (
            state.get(f"{namespace}/status") == "skipped"
        ), "Should have skipped status"
        assert (
            state.get(f"{namespace}/load_balancer_arn") == "none"
        ), "Should track 'none' value"
        assert (
            state.get(f"{namespace}/deletion_protection_disabled") == False
        ), "Should not have disabled protection"

        # Verify no ELB API calls were made
        mock_client.describe_load_balancers.assert_not_called()
        mock_client.modify_load_balancer_attributes.assert_not_called()

    except Exception as e:
        traceback.print_exc()
        pytest.fail(f"Test failed with exception: {e}")


def test_unprotect_elb_not_found(
    task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session
):
    """Test the unprotect ELB action when load balancer is not found."""
    try:
        # Create mock client that returns no load balancers
        mock_client = MagicMock()

        mock_client.describe_load_balancers.return_value = {
            "LoadBalancers": []
        }  # Empty list = load balancer not found

        mock_session.client.return_value = mock_client

        save_actions(task_payload, deploy_spec.actions)
        save_state(task_payload, {})

        # Execute the handler
        event = task_payload.model_dump()
        response = execute_handler(event, None)

        # Parse the response
        updated_payload = TaskPayload(**response)
        assert (
            updated_payload.flow_control == "failure"
        ), "Should fail when load balancer not found"

        # Load state
        state = load_state(updated_payload)
        namespace = "unprotect-elb"

        # Verify error handling
        assert state.get(f"{namespace}/status") == "error", "Should have error status"
        assert (
            "not found" in state.get(f"{namespace}/error_message", "").lower()
        ), "Error should mention load balancer not found"
        assert (
            state.get(f"{namespace}/deletion_protection_disabled") == False
        ), "Should not have disabled protection"

        # Verify modify_load_balancer_attributes was NOT called
        mock_client.modify_load_balancer_attributes.assert_not_called()

    except Exception as e:
        traceback.print_exc()
        pytest.fail(f"Test failed with exception: {e}")
