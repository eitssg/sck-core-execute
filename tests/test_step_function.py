import pytest
import json

from core_framework.models import (
    TaskPayload,
    DeploymentDetails as DeploymentDetailsClass,
)


from core_execute.stepfn import step_function_client, generate_execution_name


@pytest.fixture
def task_payload():

    task_payload = TaskPayload(
        Task="deploy",
        DeploymentDetails=DeploymentDetailsClass(
            Client="Client",
            Portfolio="Portfolio",
            Environment="Environment",
            Scope="portfolio",
            DataCenter="DataCenter",
        ),
    )

    return task_payload


def test_step_function_client(task_payload: TaskPayload):

    region = "us-east-1"

    client = step_function_client(region=region)

    assert client is not None

    event = task_payload.model_dump()

    executionName = generate_execution_name(task_payload)

    execution_arn = (
        "arn:aws:states:us-east-1:123456789012:stateMachine:my-state-machine"
    )
    result = client.start_execution(
        name=executionName,
        stateMachineArn=execution_arn,
        input=event,
    )

    assert result is not None

    print("Unit Test Execution Results:")
    print(json.dumps(result, indent=2))

    assert result["executionArn"] == execution_arn
