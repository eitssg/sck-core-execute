import traceback
import pytest
import json

from core_framework.models import (
    TaskPayload,
    DeploymentDetails as DeploymentDetailsClass,
)

from core_execute.handler import handler as execute_handler


@pytest.fixture
def real_aws(pytestconfig):
    return pytestconfig.getoption("--real-aws")


def test_lambda_handler():

    try:

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

        event = task_payload.model_dump()

        print("Event")
        print(json.dumps(event, indent=2))

        response = execute_handler(event, None)

        task_payload = TaskPayload(**response)

        assert task_payload.Task == "deploy"

    except Exception as e:
        print(traceback.format_exc())
        assert False, str(e)
