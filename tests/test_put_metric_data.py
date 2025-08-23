import traceback
import pytest
from unittest.mock import MagicMock

from datetime import datetime, timezone

import core_framework as util
from core_framework.models import TaskPayload, DeploySpec

from core_execute.actionlib.actions.aws.put_metric_data import (
    PutMetricDataActionSpec,
    PutMetricDataActionParams,
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
    Fixture to provide a deployspec data for testing.
    This can be used to mock the deployspec in tests.
    """
    params = {
        "Account": "123456789012",
        "Region": "us-west-2",
        "Namespace": "event-namespace",
        "Metrics": [
            {
                "Timestamp": "2023-10-01T12:00:00Z",
                "MetricName": "test-metric",
                "Dimensions": [
                    {"Name": "Environment", "Value": "production"},
                    {"Name": "DataCenter", "Value": "zone-1"},
                ],
                "Value": 100,
                "Unit": "Count",
            },
            {
                "Timestamp": "2023-10-01T12:05:00Z",
                "MetricName": "test-metric",
                "Dimensions": [
                    {"Name": "Environment", "Value": "production"},
                    {"Name": "DataCenter", "Value": "zone-1"},
                ],
                "Value": 200,
                "Unit": "Count",
            },
        ],
    }

    # validate the params here before we run the action
    validated_params = PutMetricDataActionParams(**params)

    # Define the action specification
    action_spec = PutMetricDataActionSpec(
        Name="event-namespace:action/test-put-metric",
        Kind="AWS::PutMetricData",
        Params=validated_params.model_dump(),
        Scope="build",
    )

    return DeploySpec(Actions=[action_spec])


def test_put_metric_data_action(
    task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session: MagicMock
):
    """Test the put metric data action successful execution."""

    try:
        # Mock the CloudWatch client
        mock_client = MagicMock()

        # Configure the CloudWatch put_metric_data method
        mock_client.put_metric_data.return_value = {
            "ResponseMetadata": {
                "RequestId": "test-request-id-123",
                "HTTPStatusCode": 200,
            }
        }

        mock_session.client.return_value = mock_client

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

        # Verify CloudWatch put_metric_data was called
        mock_client.put_metric_data.assert_called_once()

        # Get the call arguments to verify the data
        call_args = mock_client.put_metric_data.call_args

        # Verify namespace
        assert call_args[1]["Namespace"] == "event-namespace", "Namespace should match"

        # Verify metrics data
        metrics_data = call_args[1]["MetricData"]
        assert len(metrics_data) == 2, "Should have 2 metrics"

        # Verify first metric
        first_metric = metrics_data[0]
        assert first_metric["MetricName"] == "test-metric"
        assert first_metric["Value"] == 100.0
        assert first_metric["Unit"] == "Count"
        assert len(first_metric["Dimensions"]) == 2

        # Verify dimensions
        dimensions = first_metric["Dimensions"]
        assert any(
            d["Name"] == "Environment" and d["Value"] == "production"
            for d in dimensions
        )
        assert any(
            d["Name"] == "DataCenter" and d["Value"] == "zone-1" for d in dimensions
        )

        # Verify second metric
        second_metric = metrics_data[1]
        assert second_metric["MetricName"] == "test-metric"
        assert second_metric["Value"] == 200.0
        assert second_metric["Unit"] == "Count"

        # Verify state tracking
        assert state is not None, "State should not be None"

        # Check that the action completed successfully
        assert (
            state.get("event-namespace:var/test-put-metric/status") == "success"
        ), "Action should have completed successfully"
        assert (
            state.get("event-namespace:var/test-put-metric/total_metrics_sent") == 2
        ), "Should have sent 2 metrics"

        # Verify completion and error states are properly set
        assert (
            state.get("event-namespace:var/test-put-metric/metrics_count") == 2
        ), "Should track metrics count"
        assert (
            state.get("event-namespace:var/test-put-metric/namespace")
            == "event-namespace"
        ), "Should track namespace"

    except Exception as e:
        traceback.print_exc()
        pytest.fail(f"Test failed with exception: {str(e)}")
