"""Emulate how step functions and lambda run things in the background"""

import os
from datetime import datetime, timezone
import uuid
import time
import sys
import subprocess
import json

import core_logging as log

from core_framework.models import TaskPayload
from core_db.registry.client.actions import ClientActions

from core_execute.handler import handler as core_execute_handler

log_stream_name = "core-execute-cli"

log.setup(log_stream_name)


class LambdaExecutionContext(dict):
    """Emulate the lambda execution context object"""

    def __init__(self, max_lambda_time_seconds: int = 300):

        self["function_name"] = f"{log_stream_name}-function"
        self["function_version"] = "$LATEST"
        self["invoked_function_arn"] = (
            f"arn:aws:lambda:us-east-1:123456789012:function:{log_stream_name}-function"
        )
        self["memory_limit_in_mb"] = 128
        self["aws_request_id"] = str(uuid.uuid4())
        self["log_group_name"] = f"/aws/lambda/{log_stream_name}"
        self["log_stream_name"] = log_stream_name

        self.start_time = datetime.now(timezone.utc)
        self.max_lambda_time_seconds = max_lambda_time_seconds
        self.buffer = 10  # 10 second buffer
        self.max_execute_time_seconds = max_lambda_time_seconds - self.buffer

    def get_remaining_time_in_millis(self) -> int:
        """Return the remaining time in milliseconds"""

        elapsed = datetime.now(timezone.utc) - self.start_time
        remaining_time_in_seconds = (
            self.max_lambda_time_seconds - elapsed.total_seconds()
        )
        return int(remaining_time_in_seconds * 1000)

    def timeout_imminent(self) -> bool:
        """Check if the context is about to timeout.

        This function is NOT in a typical lambda context

        """
        elapsed = datetime.now(timezone.utc) - self.start_time
        return elapsed.total_seconds() > self.max_execute_time_seconds


def state_execute(task_playload: TaskPayload) -> TaskPayload:
    """Execute the state"""

    print("Running event: {}...".format(task_playload.Task), end=None)

    event = task_playload.model_dump()

    event = core_execute_handler(event, LambdaExecutionContext())

    # it is expected that the handler will return a TaskPayload dictionary
    # if it does not, this validation will return an error
    task_playload = TaskPayload(**event)

    print("done.  State: {}".format(task_playload.FlowControl))

    return task_playload


def state_wait(task_payload: TaskPayload) -> TaskPayload:

    print("Waiting for 15 seconds...", end=None)

    time.sleep(15)
    task_payload.FlowControl = "execute"

    print("continuing...")

    return task_payload


def state_success(task_payload: TaskPayload) -> TaskPayload:
    print("Executing success state...")

    result = task_payload.model_dump_json()

    with open("simulate-response.json", "w") as f:
        f.write(result)

    print(result)

    return task_payload


def state_failure(task_payload: TaskPayload) -> TaskPayload:

    print("Execution failed...")

    result = task_payload.model_dump_json()

    with open("simulate-response.json", "w") as f:
        f.write(result)

    print(result)

    return task_payload


STATE_MACHINE_FLOW = {
    "execute": state_execute,
    "wait": state_wait,
    "success": state_success,
    "failure": state_failure,
}


def emulate_state_machine(name, **kwargs):

    log.setup(name)

    task_payload = TaskPayload.from_arguments(**kwargs)
    task_payload.FlowControl = "execute"

    client_vars = ClientActions.get(client=task_payload.DeploymentDetails.Client)

    # bucket_region = client_vars['CLIENT_REGION']
    # bucket_name = '{}{}-core-automation-{}'.format(client_vars.get('SCOPE_PREFIX', ''), client_vars['CLIENT_NAME'], client_vars['CLIENT_REGION'])
    #
    # delivered_by = os.environ["DELIVERED_BY"] if "DELIVERED_BY" in os.environ else "automation"

    while True:

        fc = task_payload.FlowControl
        task_payload = STATE_MACHINE_FLOW[fc](task_payload)
        if (
            task_payload.FlowControl == "success"
            or task_payload.FlowControl == "failure"
        ):
            break

    return {"task_payload": task_payload.model_dump(), "client_vars": client_vars.data}


def generate_task_and_start(args: list):

    if len(args) < 4:
        raise ValueError("Missing task payload and name inforamtion")

    json_data = args[1]
    data = json.loads(json_data)
    name = args[3]

    emulate_state_machine(name, **data)


class MagicStepFnClient:
    """Special runner to start the process in the background"""

    def __init__(self, region: str):
        self.region = region

    def start_execution(self, **kwargs) -> dict:
        """
        Start the execution of the step function in the background.  The step function
        runs in a separate shell and disconnects from the current process allow it
        to run in the background and coninue util it completes are is forceably killed
        by the OS.

        Returns:
            dict: Information about its startup
        """
        self.start_time = datetime.now().isoformat()

        name = kwargs.get("name", None)
        self.name = name

        executionArn = kwargs.get("stateMachineArn")
        if not executionArn:
            return {"error": "No stateMachineArn provided"}

        data = kwargs.get("input")
        if not data:
            return {"error": "No input data provided"}

        try:
            # Validate the and translate to JSON
            self.task_payload = TaskPayload(**data)
            json_data = self.task_payload.model_dump_json()

            # Open a shell to run the scipt_name with the command
            # python3 script_name --task_payload "json data" --name "name"

            script_name = os.path.abspath(__file__)

            # Execute the command in a shell and disconnect the TTY
            if os.name == "nt":  # Windows
                venv_python = os.path.join(
                    os.environ["VIRTUAL_ENV"], "Scripts", "python.exe"
                )
                cmd = [
                    venv_python,
                    script_name,
                    "--task_payload",
                    json_data,
                    "--name",
                    name,
                ]

                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                startupinfo.wShowWindow = subprocess.SW_HIDE
                subprocess.Popen(
                    ["powershell", "/c"] + cmd,
                    creationflags=subprocess.CREATE_NEW_CONSOLE,
                    env=os.environ,
                    startupinfo=startupinfo,
                )
            else:  # Unix-like
                venv_python = os.path.join(os.environ["VIRTUAL_ENV"], "bin", "python")
                cmd = [
                    venv_python,
                    script_name,
                    "--task_payload",
                    json_data,
                    "--name",
                    name,
                ]

                subprocess.Popen(
                    ["nohup"] + cmd + ["&"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    env=os.environ,
                )

            return {"executionArn": executionArn, "startDate": self.start_time}

        except Exception as e:
            return {"error": str(e)}


def step_function_client(region: str) -> MagicStepFnClient:
    """Return the local step function client"""

    return MagicStepFnClient(region)


if __name__ == "__main__":

    generate_task_and_start(sys.argv[1:])
