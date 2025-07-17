import traceback
import pytest
from unittest.mock import MagicMock

import core_framework as util
from core_framework.models import TaskPayload, DeploySpec

from core_execute.actionlib.actions.aws.rds.modify_db_instance import ModifyDbInstanceActionSpec

from core_execute.execute import save_state, save_actions

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
def minimal_deploy_spec():
    """Minimal parameters for basic modify operation"""
    params = {
        "Account": "test-db-account",
        "Region": "us-east-1",
        "ApiParams": {"DBInstanceIdentifier": "test-db-instance", "DBInstanceClass": "db.t3.small", "ApplyImmediately": True},
    }
    modify_db_instance_action = ModifyDbInstanceActionSpec(**{"params": params})
    return DeploySpec(**{"actions": [modify_db_instance_action]})


@pytest.fixture
def deploy_spec(task_payload: TaskPayload):
    """
    Fixture to provide a deployspec data for testing.
    This can be used to mock the deployspec in tests.
    """
    params = {
        "Account": "test-db-account",
        "Region": "us-east-1",
        "ApiParams": {
            "DBInstanceIdentifier": "test-db-instance",
            "DBInstanceClass": "db.t3.micro",
            "AllocatedStorage": 20,
            "StorageType": "gp2",
            "Engine": "mysql",
            "EngineVersion": "8.0.35",
            "MasterUsername": "admin",
            "MasterUserPassword": "temppassword123",
            "VpcSecurityGroupIds": ["sg-1234567890abcdef0"],
            "DBSubnetGroupName": "test-subnet-group",
            "MultiAZ": False,
            "PubliclyAccessible": False,
            "StorageEncrypted": True,
            "BackupRetentionPeriod": 7,
            "PreferredBackupWindow": "03:00-04:00",
            "PreferredMaintenanceWindow": "sun:04:00-sun:05:00",
            "ApplyImmediately": True,
            "DeletionProtection": False,
            "Tags": [{"Key": "Environment", "Value": "production"}, {"Key": "Application", "Value": "test-app"}],
        },
    }

    # Define the action specifications with the modify DB instance action
    modify_db_instance_action = ModifyDbInstanceActionSpec(**{"params": params})
    return DeploySpec(**{"actions": [modify_db_instance_action]})


def test_lambda_handler(task_payload: TaskPayload, deploy_spec: DeploySpec, mock_session):

    try:

        mock_client = MagicMock()
        # Update the modify_db_instance mock to include sample pending modifications
        mock_client.modify_db_instance.return_value = {
            "DBInstance": {
                "DBInstanceIdentifier": "test-db-instance",
                "DBInstanceClass": "db.t3.micro",
                "PendingModifiedValues": {
                    "AllocatedStorage": 25,
                    "DBInstanceClass": "db.t3.medium"
                },
            },
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        mock_client.describe_db_instances.return_value = {
            "DBInstances": [
                {
                    "DBInstanceIdentifier": "test-db-instance",
                    "DBInstanceClass": "db.t3.micro",
                    "PendingModifiedValues": None,  # No pending modifications after initial call
                }
            ]
        }
        mock_session.client.return_value = mock_client

        save_actions(task_payload, deploy_spec.actions)
        save_state(task_payload, {})

        event = task_payload.model_dump()

        # Call the execute handler with the task payload and deploy spec
        response = execute_handler(event, None)

        # Check if the response is as expected
        assert response is not None, "Response should not be None"
        assert isinstance(response, dict), "Response should be a dictionary"

        # Parse the response back into TaskPayload
        task_payload = TaskPayload(**response)

        # Validate the flow control in the task payload
        assert task_payload.flow_control == "success", "Expected flow_control to be 'success'"

        # Additional checks can be added here as needed

    except Exception as e:
        print(traceback.format_exc())
        assert False, str(e)
