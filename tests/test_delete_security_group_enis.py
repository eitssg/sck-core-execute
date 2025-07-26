from typing import Any
import traceback
from unittest import mock
import pytest
from unittest.mock import MagicMock
from datetime import datetime, timezone
from botocore.exceptions import ClientError

from core_framework.models import TaskPayload, DeploySpec

from core_execute.actionlib.actions.aws.delete_security_group_enis import (
    DeleteSecurityGroupEnisActionSpec,
    DeleteSecurityGroupEnisActionParams,
)
from core_execute.handler import handler as execute_handler
from core_execute.execute import save_actions, save_state, load_state

from .aws_fixtures import *


# Scope this so it's created fresh for each test
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
    Parameters are fore: DeleteSecurityGroupEnisActionParams
    """
    spec: dict[str, Any] = {
        "Params": {"Account": "154798051514", "Region": "ap-southeast-1", "SecurityGroupId": "sg-1234567890abcdef0"}
    }

    action_spec = DeleteSecurityGroupEnisActionSpec(**spec)

    deploy_spec: dict[str, Any] = {"actions": [action_spec]}

    return DeploySpec(**deploy_spec)


def test_delete_security_group_enis(task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session):

    try:
        creation_time = datetime(2023, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_client = MagicMock()

        # Create a sequence of return values for describe_network_interfaces
        # The execute_handler will call this multiple times internally via _execute() then _check()

        # First call (_execute): 3 ENIs (1 available, 1 in-use detachable, 1 hyperplane-managed)
        first_call_response = {
            "NetworkInterfaces": [
                {
                    "NetworkInterfaceId": "eni-1234567890abcdef0",
                    "Status": "available",
                    "Groups": [{"GroupId": "sg-1234567890abcdef0"}],
                    "Description": "Available ENI for testing",
                    "PrivateIpAddress": "10.0.1.100",
                    "SubnetId": "subnet-1234567890abcdef0",
                    "VpcId": "vpc-1234567890abcdef0",
                    "NetworkInterfaceType": "interface",
                    "OwnerId": "154798051514",
                },
                {
                    "NetworkInterfaceId": "eni-0987654321fedcba0",
                    "Status": "in-use",
                    "Groups": [{"GroupId": "sg-1234567890abcdef0"}],
                    "Description": "In-use ENI attached to instance",
                    "PrivateIpAddress": "10.0.1.101",
                    "SubnetId": "subnet-1234567890abcdef0",
                    "VpcId": "vpc-1234567890abcdef0",
                    "NetworkInterfaceType": "interface",
                    "OwnerId": "154798051514",
                    "Attachment": {
                        "AttachmentId": "eni-attach-1234567890abcdef0",
                        "InstanceId": "i-1234567890abcdef0",
                        "InstanceOwnerId": "154798051514",
                        "Status": "attached",
                        "AttachTime": creation_time,
                        "DeleteOnTermination": False,
                    },
                },
                {
                    "NetworkInterfaceId": "eni-abcdef0123456789",
                    "Status": "in-use",
                    "Groups": [{"GroupId": "sg-1234567890abcdef0"}],
                    "Description": "Hyperplane-managed ENI",
                    "PrivateIpAddress": "10.0.1.102",
                    "SubnetId": "subnet-1234567890abcdef0",
                    "VpcId": "vpc-1234567890abcdef0",
                    "NetworkInterfaceType": "interface",
                    "OwnerId": "154798051514",
                    "Attachment": {
                        "AttachmentId": "eni-attach-abcdef0123456789",
                        "InstanceId": "i-abcdef0123456789",
                        "InstanceOwnerId": "amazon-aws",  # Hyperplane-managed
                        "Status": "attached",
                        "AttachTime": creation_time,
                        "DeleteOnTermination": False,
                    },
                },
            ]
        }

        # Second call (_check): 1 ENI (the detached one, now available for deletion)
        second_call_response = {
            "NetworkInterfaces": [
                {
                    "NetworkInterfaceId": "eni-0987654321fedcba0",
                    "Status": "available",  # Now available after detachment
                    "Groups": [{"GroupId": "sg-1234567890abcdef0"}],
                    "Description": "Previously in-use ENI, now available",
                    "PrivateIpAddress": "10.0.1.101",
                    "SubnetId": "subnet-1234567890abcdef0",
                    "VpcId": "vpc-1234567890abcdef0",
                    "NetworkInterfaceType": "interface",
                    "OwnerId": "154798051514",
                }
            ]
        }

        # Third call (_check): 0 ENIs (all processed ENIs are gone)
        third_call_response = {"NetworkInterfaces": []}

        # Set up the mock to return different responses on each internal call
        mock_client.describe_network_interfaces.side_effect = [
            first_call_response,  # _execute() call
            second_call_response,  # First _check() call
            third_call_response,  # Second _check() call (if needed)
        ]

        # Mock successful operations
        mock_client.delete_network_interface.return_value = {
            "ResponseMetadata": {"RequestId": "12345678-1234-1234-1234-123456789012", "HTTPStatusCode": 200}
        }

        mock_client.detach_network_interface.return_value = {
            "ResponseMetadata": {"RequestId": "12345678-1234-1234-1234-123456789012", "HTTPStatusCode": 200}
        }

        mock_session.client.return_value = mock_client

        save_actions(task_payload, deploy_spec.actions)
        save_state(task_payload, {})

        # SINGLE CALL - execute_handler manages iterations internally
        print("🔄 Running execute_handler (manages internal iterations)...")
        event = task_payload.model_dump()
        result = execute_handler(event, None)
        task_payload = TaskPayload(**result)

        # Should be complete after internal iterations
        assert task_payload.flow_control == "success", f"Expected flow_control to be 'success', got '{task_payload.flow_control}'"

        state = load_state(task_payload)
        action_name = "action-aws-deletesecuritygroupenis-name"

        # Verify final completion state
        assert state[f"{action_name}/TotalEnisFound"] == 3
        assert state[f"{action_name}/DeletedEniCount"] == 2  # Both available ENIs deleted
        assert state[f"{action_name}/DetachedEniCount"] == 1  # One ENI was detached
        assert state[f"{action_name}/SkippedEniCount"] == 1  # Hyperplane ENI skipped
        assert state[f"{action_name}/InUseEniCount"] == 0  # No ENIs waiting anymore
        assert state[f"{action_name}/DeletionCompleted"] is True
        assert state[f"{action_name}/DeletionResult"] == "SUCCESS"
        assert state[f"{action_name}/StatusCode"] == "complete"

        # Verify all EC2 operations were called
        assert mock_client.describe_network_interfaces.call_count == 2  # Called in _execute and _check
        assert mock_client.delete_network_interface.call_count == 2  # Two ENIs deleted
        assert mock_client.detach_network_interface.call_count == 1  # One ENI detached

        # Verify specific operation calls
        delete_calls = mock_client.delete_network_interface.call_args_list
        delete_eni_ids = [call[1]["NetworkInterfaceId"] for call in delete_calls]
        assert "eni-1234567890abcdef0" in delete_eni_ids  # Available ENI deleted immediately
        assert "eni-0987654321fedcba0" in delete_eni_ids  # Detached ENI deleted later

        mock_client.detach_network_interface.assert_called_with(AttachmentId="eni-attach-1234567890abcdef0", Force=True)

        # Verify final state tracking
        deleted_enis = state[f"{action_name}/DeletedEnis"]
        assert len(deleted_enis) == 2
        deleted_eni_ids = [eni["EniId"] for eni in deleted_enis]
        assert "eni-1234567890abcdef0" in deleted_eni_ids  # Originally available
        assert "eni-0987654321fedcba0" in deleted_eni_ids  # Originally detached

        skipped_enis = state[f"{action_name}/SkippedEnis"]
        assert len(skipped_enis) == 1
        assert skipped_enis[0]["EniId"] == "eni-abcdef0123456789"
        assert skipped_enis[0]["Reason"] == "Hyperplane-managed"

        print("✅ All ENI deletion validations passed")
        print(f"📊 Security Group: {state.get(f'{action_name}/SecurityGroupId')}")
        print(f"📊 Total ENIs Found: {state.get(f'{action_name}/TotalEnisFound')}")
        print(f"📊 ENIs Deleted: {state.get(f'{action_name}/DeletedEniCount')}")
        print(f"📊 ENIs Skipped: {state.get(f'{action_name}/SkippedEniCount')}")

    except Exception as e:
        print(f"❌ An error occurred: {e}")
        traceback.print_exc()
        pytest.fail(f"Test failed due to an exception: {e}")


def test_delete_security_group_enis_immediate_completion(task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session):
    """Test immediate completion when all ENIs disappear after first iteration"""

    try:
        creation_time = datetime(2023, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_client = MagicMock()

        # First call: 2 ENIs
        first_call_response = {
            "NetworkInterfaces": [
                {
                    "NetworkInterfaceId": "eni-1234567890abcdef0",
                    "Status": "available",
                    "Groups": [{"GroupId": "sg-1234567890abcdef0"}],
                    "Description": "Available ENI for testing",
                    "PrivateIpAddress": "10.0.1.100",
                    "SubnetId": "subnet-1234567890abcdef0",
                    "VpcId": "vpc-1234567890abcdef0",
                    "NetworkInterfaceType": "interface",
                    "OwnerId": "154798051514",
                },
                {
                    "NetworkInterfaceId": "eni-0987654321fedcba0",
                    "Status": "in-use",
                    "Groups": [{"GroupId": "sg-1234567890abcdef0"}],
                    "Description": "In-use ENI attached to instance",
                    "PrivateIpAddress": "10.0.1.101",
                    "SubnetId": "subnet-1234567890abcdef0",
                    "VpcId": "vpc-1234567890abcdef0",
                    "NetworkInterfaceType": "interface",
                    "OwnerId": "154798051514",
                    "Attachment": {
                        "AttachmentId": "eni-attach-1234567890abcdef0",
                        "InstanceId": "i-1234567890abcdef0",
                        "InstanceOwnerId": "154798051514",
                        "Status": "attached",
                        "AttachTime": creation_time,
                        "DeleteOnTermination": False,
                    },
                },
            ]
        }

        # Second call: 0 ENIs (AWS cleaned them up)
        second_call_response = {"NetworkInterfaces": []}

        mock_client.describe_network_interfaces.side_effect = [first_call_response, second_call_response]

        mock_client.delete_network_interface.return_value = {
            "ResponseMetadata": {"RequestId": "12345678-1234-1234-1234-123456789012", "HTTPStatusCode": 200}
        }

        mock_client.detach_network_interface.return_value = {
            "ResponseMetadata": {"RequestId": "12345678-1234-1234-1234-123456789012", "HTTPStatusCode": 200}
        }

        mock_session.client.return_value = mock_client

        save_actions(task_payload, deploy_spec.actions)
        save_state(task_payload, {})

        # Single call - execute_handler manages iterations
        event = task_payload.model_dump()
        result = execute_handler(event, None)
        task_payload = TaskPayload(**result)

        assert task_payload.flow_control == "success"

        state = load_state(task_payload)
        action_name = "action-aws-deletesecuritygroupenis-name"

        assert state[f"{action_name}/DeletionCompleted"] is True
        assert state[f"{action_name}/DeletionResult"] == "SUCCESS"
        assert state[f"{action_name}/InUseEniCount"] == 0

        print("✅ Immediate completion test passed")

    except Exception as e:
        print(f"❌ An error occurred: {e}")
        traceback.print_exc()
        pytest.fail(f"Test failed due to an exception: {e}")
