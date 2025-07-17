from typing import Any
import traceback
from unittest import mock
import pytest
from unittest.mock import MagicMock
from datetime import datetime, timezone
from botocore.exceptions import ClientError

from core_framework.models import TaskPayload, DeploySpec

from core_execute.actionlib.actions.aws.create_stack import CreateStackActionSpec, CreateStackActionParams
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
    Parameters are fore: CreateStackActionParams
    """
    spec: dict[str, Any] = {
        "Params": {
            "Account": "154798051514",
            "Region": "ap-southeast-1",
            "StackName": "my-application-stack",
            "TemplateUrl": "s3://my-bucket/my-template.yaml",
            "StackParameters": {"Build": "ver1.0", "Environment": "production"},
            "Tags": {"App": "My application", "Environment": "production"},
            "TimeoutInMinutes": 15,
        }
    }

    action_spec = CreateStackActionSpec(**spec)

    deploy_spec: dict[str, Any] = {"actions": [action_spec]}

    return DeploySpec(**deploy_spec)


def test_create_stack_action(task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session):

    try:

        mock_client = MagicMock()

        # Mock describe_stacks - first call returns stack doesn't exist, second call after creation returns complete
        mock_client.describe_stacks.side_effect = [
            # First call - stack doesn't exist (triggers create)
            ClientError(
                error_response={
                    "Error": {"Code": "ValidationError", "Message": "Stack with id my-application-stack does not exist"}
                },
                operation_name="DescribeStacks",
            ),
            # Second call - stack exists and is complete
            {
                "Stacks": [
                    {
                        "StackId": "arn:aws:cloudformation:ap-southeast-1:154798051514:stack/my-application-stack/12345678-1234-1234-1234-123456789012",
                        "StackName": "my-application-stack",
                        "StackStatus": "CREATE_COMPLETE",
                        "StackStatusReason": "Stack creation completed successfully",
                        "CreationTime": datetime(2023, 10, 1, 12, 0, 0, tzinfo=timezone.utc),
                        "Description": "My application stack",
                        "Tags": [
                            {"Key": "App", "Value": "My application"},
                            {"Key": "Environment", "Value": "production"},
                            {"Key": "DeliveredBy", "Value": "simple-cloud-kit"},
                        ],
                        "Outputs": [
                            {
                                "OutputKey": "MyOutput",
                                "OutputValue": "OutputValue",
                                "Description": "My output description",
                            },
                            {
                                "OutputKey": "ApplicationUrl",
                                "OutputValue": "https://my-app.example.com",
                                "Description": "Application URL",
                            },
                        ],
                    }
                ]
            },
        ]

        # Mock validate_template - returns successful validation
        mock_client.validate_template.return_value = {
            "Parameters": [
                {"ParameterKey": "Build", "DefaultValue": "", "NoEcho": False},
                {"ParameterKey": "Environment", "DefaultValue": "", "NoEcho": False},
            ],
            "Description": "CloudFormation template for my application",
            "Capabilities": ["CAPABILITY_IAM"],
            "CapabilitiesReason": "The template contains IAM resources",
        }

        # Mock create_stack - returns stack ID
        mock_client.create_stack.return_value = {
            "StackId": "arn:aws:cloudformation:ap-southeast-1:154798051514:stack/my-application-stack/12345678-1234-1234-1234-123456789012"
        }

        # Mock list_stack_resources - returns resource summary
        mock_client.list_stack_resources.return_value = {
            "StackResourceSummaries": [
                {
                    "LogicalResourceId": "MyS3Bucket",
                    "PhysicalResourceId": "my-app-bucket-123456",
                    "ResourceType": "AWS::S3::Bucket",
                    "ResourceStatus": "CREATE_COMPLETE",
                    "LastUpdatedTimestamp": datetime(2023, 10, 1, 12, 5, 0, tzinfo=timezone.utc),
                },
                {
                    "LogicalResourceId": "MyLambdaFunction",
                    "PhysicalResourceId": "my-app-lambda-function",
                    "ResourceType": "AWS::Lambda::Function",
                    "ResourceStatus": "CREATE_COMPLETE",
                    "LastUpdatedTimestamp": datetime(2023, 10, 1, 12, 8, 0, tzinfo=timezone.utc),
                },
                {
                    "LogicalResourceId": "MyApiGateway",
                    "PhysicalResourceId": "abc123def456",
                    "ResourceType": "AWS::ApiGateway::RestApi",
                    "ResourceStatus": "CREATE_COMPLETE",
                    "LastUpdatedTimestamp": datetime(2023, 10, 1, 12, 10, 0, tzinfo=timezone.utc),
                },
            ]
        }

        # Mock describe_stack_events - returns stack events for monitoring
        mock_client.describe_stack_events.return_value = {
            "StackEvents": [
                {
                    "StackId": "arn:aws:cloudformation:ap-southeast-1:154798051514:stack/my-application-stack/12345678-1234-1234-1234-123456789012",
                    "EventId": "event-123",
                    "StackName": "my-application-stack",
                    "LogicalResourceId": "my-application-stack",
                    "PhysicalResourceId": "arn:aws:cloudformation:ap-southeast-1:154798051514:stack/my-application-stack/12345678-1234-1234-1234-123456789012",
                    "ResourceType": "AWS::CloudFormation::Stack",
                    "Timestamp": datetime(2023, 10, 1, 12, 10, 0, tzinfo=timezone.utc),
                    "ResourceStatus": "CREATE_COMPLETE",
                }
            ]
        }

        # Mock detect_stack_drift - returns drift detection ID
        mock_client.detect_stack_drift.return_value = {"StackDriftDetectionId": "drift-detection-123456"}

        # Mock delete_stack - for rollback scenarios
        mock_client.delete_stack.return_value = {
            "ResponseMetadata": {"RequestId": "12345678-1234-1234-1234-123456789012", "HTTPStatusCode": 200}
        }

        # Mock cancel_update_stack - for cancellation scenarios
        mock_client.cancel_update_stack.return_value = {
            "ResponseMetadata": {"RequestId": "12345678-1234-1234-1234-123456789012", "HTTPStatusCode": 200}
        }

        mock_session.client = MagicMock(return_value=mock_client)

        save_actions(task_payload, deploy_spec.actions)
        save_state(task_payload, {})

        event = task_payload.model_dump()

        result = execute_handler(event, None)

        assert result is not None, "Result should not be None"
        assert isinstance(result, dict), "Result should be a dictionary"

        task_payload = TaskPayload(**result)

        # Validate the flow control in the task payload
        assert task_payload.flow_control == "success", "Expected flow_control to be 'success'"

        state = load_state(task_payload)

        # Verify CloudFormation client method calls
        mock_client.validate_template.assert_called_once_with(TemplateURL="s3://my-bucket/my-template.yaml")

        mock_client.create_stack.assert_called_once()
        create_call_args = mock_client.create_stack.call_args[1]
        assert create_call_args["StackName"] == "my-application-stack"
        assert create_call_args["TemplateURL"] == "s3://my-bucket/my-template.yaml"
        assert create_call_args["TimeoutInMinutes"] == 15

        mock_client.list_stack_resources.assert_called_once()
        mock_client.detect_stack_drift.assert_called_once()

        # Validate state outputs that should be set by the action
        action_name = "action-aws-createstack-name"

        # Check basic parameters are stored in state
        assert f"{action_name}/StackName" in state
        assert state[f"{action_name}/StackName"] == "my-application-stack"

        assert f"{action_name}/TemplateUrl" in state
        assert state[f"{action_name}/TemplateUrl"] == "s3://my-bucket/my-template.yaml"

        assert f"{action_name}/Region" in state
        assert state[f"{action_name}/Region"] == "ap-southeast-1"

        assert f"{action_name}/Account" in state
        assert state[f"{action_name}/Account"] == "154798051514"

        # Check stack creation results
        assert f"{action_name}/StackId" in state
        assert "arn:aws:cloudformation" in state[f"{action_name}/StackId"]

        assert f"{action_name}/StackOperation" in state
        assert state[f"{action_name}/StackOperation"] == "CREATE"

        assert f"{action_name}/StackStatus" in state
        assert state[f"{action_name}/StackStatus"] == "CREATE_COMPLETE"

        assert f"{action_name}/StackOperationCompleted" in state
        assert state[f"{action_name}/StackOperationCompleted"] is True

        assert f"{action_name}/StackCreationStarted" in state
        assert state[f"{action_name}/StackCreationStarted"] is True

        # Check stack outputs are captured
        assert f"{action_name}/StackOutputCount" in state
        assert state[f"{action_name}/StackOutputCount"] == 2

        assert f"{action_name}/MyOutput" in state
        assert state[f"{action_name}/MyOutput"] == "OutputValue"

        assert f"{action_name}/ApplicationUrl" in state
        assert state[f"{action_name}/ApplicationUrl"] == "https://my-app.example.com"

        # Check resource summary
        assert f"{action_name}/StackResourceCount" in state
        assert state[f"{action_name}/StackResourceCount"] == 3

        assert f"{action_name}/StackResourceTypes" in state
        resource_types = state[f"{action_name}/StackResourceTypes"]
        assert resource_types["AWS::S3::Bucket"] == 1
        assert resource_types["AWS::Lambda::Function"] == 1
        assert resource_types["AWS::ApiGateway::RestApi"] == 1

        # Check drift detection
        assert f"{action_name}/DriftDetectionId" in state
        assert state[f"{action_name}/DriftDetectionId"] == "drift-detection-123456"

        # Check metadata
        assert f"{action_name}/StackDescription" in state
        assert state[f"{action_name}/StackDescription"] == "My application stack"

        assert f"{action_name}/StatusCode" in state
        assert state[f"{action_name}/StatusCode"] == "complete"

        # Check stack exists flag
        assert f"{action_name}/StackExists" in state
        assert state[f"{action_name}/StackExists"] is False  # Was False initially, then created

        # Verify events were captured
        assert f"{action_name}/StackEventsCount" in state
        assert state[f"{action_name}/StackEventsCount"] == 1

        print("✅ All CloudFormation stack creation validations passed")
        print(f"📊 Stack ID: {state.get(f'{action_name}/StackId')}")
        print(f"📊 Operation: {state.get(f'{action_name}/StackOperation')}")
        print(f"📊 Status: {state.get(f'{action_name}/StackStatus')}")
        print(f"📊 Resource Count: {state.get(f'{action_name}/StackResourceCount')}")
        print(f"📊 Output Count: {state.get(f'{action_name}/StackOutputCount')}")

    except Exception as e:
        traceback.print_exc()
        pytest.fail(f"Test failed with exception: {e}")
