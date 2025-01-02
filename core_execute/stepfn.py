"""Emulate how step functions and lambda run things in the background"""

import sys
import os
from datetime import datetime, timezone
import uuid
import time
import subprocess
import json
import argparse
from dotenv import load_dotenv

load_dotenv()

import core_logging as log  # noqa E402

import core_framework as util  # noqa E402

from core_framework.models import TaskPayload  # noqa E402
from core_db.registry.client.actions import ClientActions  # noqa E402

from core_execute.handler import handler as core_execute_handler  # noqa E402

log_stream_name = "core-execute-cli"

log.setup(log_stream_name)

SLEEP_TIME_IN_SECONDS = 15


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


def generate_execution_name(task_playload: TaskPayload) -> str:
    """
    Generate a unique name for the execution.

    This will create a name based on deployment details and the current time.

    It will concatenate the following fields:

    - Task
    - Portfolio
    - App
    - BranchShortName
    - Build
    - Current time in seconds

    The ressult will be, for example:  ``deploy-portfolio-app-branch-build-1234567890``

    Args:
        task_playload (TaskPayload): The task paload to generate the name for

    Returns:
        str: The name of the execution
    """
    dd = task_playload.DeploymentDetails
    return "-".join(
        [
            task_playload.Task.lower(),
            dd.Portfolio.lower(),
            dd.App or "",
            dd.BranchShortName or "",
            dd.Build or "",
            str(int(time.time())),
        ]
    ).lower()


def state_execute(task_playload: TaskPayload) -> TaskPayload:
    """Execute the state"""

    log.trace("Entering state_execute")

    log.info("State Execute Running event: {}", task_playload.Task)

    print("Running event: {}...".format(task_playload.Task), end=None)

    event = task_playload.model_dump()
    event = core_execute_handler(event, LambdaExecutionContext())
    task_playload = TaskPayload(**event)

    log.info("State Execute complete with response: {}", task_playload.FlowControl)

    print("done.  State: {}".format(task_playload.FlowControl))

    log.trace("Exiting state_execute")

    return task_playload


def state_wait(task_payload: TaskPayload) -> TaskPayload:

    log.trace("Entering state_wait")

    print(f"Waiting for {SLEEP_TIME_IN_SECONDS} seconds...", end=None)

    log.info("State Wait for {} seconds", SLEEP_TIME_IN_SECONDS)

    time.sleep(SLEEP_TIME_IN_SECONDS)

    # We have waited, so back to execute
    task_payload.FlowControl = "execute"

    log.info("State Wait continuing execution of Task: {}", task_payload.Task)

    print("continuing...")

    log.trace("Exiting state_wait")

    return task_payload


def state_success(task_payload: TaskPayload) -> TaskPayload:

    log.trace("Entering state_success")

    print("Executing success state...")

    log.info("State Success for task: {}", task_payload.Task)

    log.debug("State Success details: ", details=task_payload.model_dump())

    print("Success!")

    log.trace("Exiting state_success")

    return task_payload


def state_failure(task_payload: TaskPayload) -> TaskPayload:

    log.trace("Entering state_failure")

    print("Execution failed...")

    result = task_payload.model_dump()

    log.info("Execution failed with response: ", details=result)

    print(json.dumps(result, indent=2))

    log.trace("Exiting state_failure")

    return task_payload


def emulate_state_machine(name, task_payload: TaskPayload) -> None:

    log.trace("Entering emulate_state_machine")

    task_payload.FlowControl = "execute"

    log.info("Starting emulation for {}", name)
    log.debug("Task Payload:", details=task_payload.model_dump())

    while True:

        log.setup(name)

        fc = task_payload.FlowControl

        if fc == "execute":
            task_payload = state_execute(task_payload)
            continue

        if fc == "wait":
            task_payload = state_wait(task_payload)
            continue

        if fc == "success":
            state_success(task_payload)
            break

        if fc == "failure":
            state_failure(task_payload)
            break

    # task_payload.FlowControl = "success" | "failure"
    log.debug("Emulation complete with response: ", details=task_payload.model_dump())

    log.trace("Exiting emulate_state_machine")


def generate_task_and_start(args) -> None:

    try:
        parser = argparse.ArgumentParser()

        parser.add_argument("--task-payload", type=str, required=True)
        parser.add_argument("--name", type=str, required=True)
        args = parser.parse_args(args)

        log.trace("Geneate_task_and_start")

        json_data = args.task_payload
        name = args.name

        data = json.loads(json_data)
        task_payload = TaskPayload(**data)

        log.debug("Starting execution with name: {} and data: {}", name, data)

        emulate_state_machine(name, task_payload)

        log.trace("Generate_task_and_start complete")

    except Exception as e:
        log.error("Error in generate_task_and_start: {}", str(e))
        sys.exit(1)


class MagicStepFnClient:
    """Special runner to start the process in the background"""

    def __init__(self, region: str):
        self.region = region

    def __run_in_windows(self, task_payload: TaskPayload) -> None:

        task_payload_str = task_payload.model_dump_json()

        # Properly quote the JSON string
        task_payload_str_quoted = task_payload_str.replace('"', '\\"')

        script_name = os.path.abspath(__file__)
        venv_python = os.path.join(os.environ["VIRTUAL_ENV"], "Scripts", "python.exe")

        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = subprocess.SW_HIDE

        cmd = [
            "powershell",
            "/c",
            "Start-Process",
            f"{venv_python}",
            "-ArgumentList",
            f"'{script_name}', '--task-payload', '{task_payload_str_quoted}', '--name', '{self.name}'",
            "-NoNewWindow",
            "-PassThru",
        ]

        log.debug("Starting process with command: {}", cmd)

        p_result = subprocess.run(
            cmd,
            creationflags=subprocess.CREATE_NEW_CONSOLE,
            env=os.environ,
            startupinfo=startupinfo,
        )

        log.info("Started process with PID: {}", p_result)

    def __run_in_unix(self, task_payload: TaskPayload) -> None:

        task_payload_str = task_payload.model_dump_json()

        # Properly quote the JSON string
        task_payload_str_quoted = task_payload_str.replace('"', '\\"')

        script_name = os.path.abspath(__file__)
        venv_python = os.path.join(os.environ["VIRTUAL_ENV"], "bin", "python")
        cmd = [
            venv_python,
            script_name,
            "--task_payload",
            task_payload_str_quoted,
            "--name",
            self.name,
        ]

        log.debug("Starting process with command: {}", cmd)

        subprocess.run(
            ["nohup"] + cmd + ["&"],
            env=os.environ,
        )

    def __run_in_console(self, task_payload: TaskPayload) -> None:

        log.debug("Starting process in console")

        emulate_state_machine(self.name, task_payload)

    def start_execution(self, **kwargs) -> dict:
        """
        Start the execution of the step function in the background.  The step function
        runs in a separate shell and disconnects from the current process allow it
        to run in the background and coninue util it completes are is forceably killed
        by the OS.

        Args:
            **kwargs: The arguments to pass to the step

                * stateMachineArn: The ARN of the state machine
                * input: The input data for the state machine (task_payload)
                * name: The name of the state machine

        Returns:
            dict: Information about its startup
        """
        try:
            log.trace("Starting execution with kwargs: ", details=kwargs)

            self.start_time = datetime.now().isoformat()

            self.name = kwargs.get("name", None)
            if not self.name:
                raise ValueError("No name provided")

            self.executionArn = kwargs.get("stateMachineArn")
            if not self.executionArn:
                raise ValueError("No stateMachineArn provided")

            self.data = kwargs.get("input")
            if not self.data:
                raise ValueError("No input data provided")

            log.debug(
                "Starting execution of {} arn={} at: {}",
                self.name,
                self.executionArn,
                self.start_time,
            )

            # Validate the and translate to JSON
            task_payload = TaskPayload(**self.data)

            log.set_identity(task_payload.Identity)

            # Open a shell to run the scipt_name with the command
            # python3 script_name --task_payload "json data" --name "name"

            log.info("Starting step function emulator for {}", self.name)

            typ = os.getenv("CONSOLE", os.name)

            # Execute the command in a shell and disconnect the TTY
            if typ == "interactive":
                self.__run_in_console(task_payload)
            elif typ == "nt":  # Windows
                self.__run_in_windows(task_payload)
            else:  # Unix-like
                self.__run_in_unix(task_payload)

            exec_result: dict = {
                "executionArn": self.executionArn,
                "startDate": self.start_time,
            }

            log.debug("Execution information: ", details=exec_result)

            log.trace("Stert Execution complete")

            return exec_result

        except Exception as e:
            return {"error": str(e)}


def step_function_client(**kwargs) -> MagicStepFnClient:
    """Return the local step function client"""

    region = kwargs.get("region", util.get_region())

    return MagicStepFnClient(region)


if __name__ == "__main__":
    generate_task_and_start(sys.argv[1:])
