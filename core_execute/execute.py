"""Enhanced action execution engine with rerun support and lifecycle hooks.

This module provides the core execution logic for the Simple Cloud Kit automation
framework. It handles action dependency resolution, execution orchestration,
state management, and lifecycle hook processing.

Key Features:
- Rerun support via INIT flow control
- Lifecycle hook execution for completed actions
- Enhanced dependency management
- Critical vs non-critical failure handling
- Action execution tracking per Step Function run
- Comprehensive error handling and logging
"""

import io
import time
import time
import inflect
from typing import Any
from datetime import datetime, timezone

import core_logging as log
import core_framework as util
from core_framework.models import TaskPayload, ActionResource
from core_helper.magic import MagicS3Client

from .actionlib.helper import Helper, FlowControl, ActionStatus
from .actionlib.action import BaseAction

_p = inflect.engine()

# When the lambda function is booted and the python module is loaded, we'll get a __bootup_time__
__bootup_time__ = datetime.now(timezone.utc).timestamp()
__max_runtime__ = 10 * 60 * 1000  # 10 minutes in milliseconds


def timeout_imminent(context: Any | None = None) -> bool:
    """
    Check if the Lambda function is about to timeout.

    The function considers timeout imminent if less than 10 seconds remaining.
    If no context is provided, it assumes we're not running in a Lambda environment
    and calculates based on the maximum runtime of 10 minutes from bootup time.

    :param context: Lambda context object providing runtime information
    :type context: Any | None
    :return: True if the Lambda function is about to timeout, False otherwise
    :rtype: bool
    """
    # Timeout threshold is 10 seconds (in milliseconds)
    timeout_threshold_ms = 10000

    # Check if we're running in Lambda environment
    if context and hasattr(context, "get_remaining_time_in_millis"):
        # Lambda environment - use the actual remaining time
        remaining_time_in_millis = context.get_remaining_time_in_millis()
        log.trace("Lambda context remaining time: {} ms", remaining_time_in_millis)

    else:
        # Local/standalone mode - emulate get_remaining_time_in_millis()
        current_time = datetime.now(timezone.utc).timestamp()
        elapsed_time_ms = int((current_time - __bootup_time__) * 1000)
        remaining_time_in_millis = __max_runtime__ - elapsed_time_ms

        log.trace(
            "Local mode - elapsed: {} ms, remaining: {} ms",
            elapsed_time_ms,
            remaining_time_in_millis,
        )

        # Ensure we don't return negative values
        if remaining_time_in_millis < 0:
            remaining_time_in_millis = 0

    # Consider timeout imminent if less than threshold remaining
    is_imminent = remaining_time_in_millis < timeout_threshold_ms

    if is_imminent:
        log.debug(
            "Timeout imminent: {} ms remaining (threshold: {} ms)",
            remaining_time_in_millis,
            timeout_threshold_ms,
        )

    return is_imminent


def run_state_machine(action_helper: Helper, context: Any | None) -> FlowControl:
    """
    Execute the enhanced state machine with parallel execution, rerun support and lifecycle hooks.

    This function orchestrates the execution of actions using the Helper's enhanced
    tracking and dependency management. Uses parallel execution for independent actions
    like CloudFormation stacks, executes lifecycle hooks when actions complete, and
    handles both critical and non-critical failures.

    Key Features:
    - Parallel execution for independent CloudFormation stacks
    - Smart threading decision based on action characteristics
    - Single execution per Step Function run (prevents email spam)
    - Lifecycle hook execution for completed actions
    - Critical vs non-critical failure handling
    - Enhanced progress tracking and logging
    - Timeout-aware execution

    :param action_helper: Enhanced Helper managing actions and their states
    :type action_helper: Helper
    :param context: Lambda context for timeout management
    :type context: Any | None
    :return: Flow control state for Step Function continuation
    :rtype: FlowControl
    """
    log.trace("Entering enhanced run_state_machine (threading={})", action_helper.use_threading)

    # Get initial execution summary
    summary = action_helper.get_execution_summary()
    log.info("Execution summary at start: {}", summary)

    # Check for critical failures first - fail fast
    if action_helper.has_critical_failures():
        log.error("Critical failures detected, stopping execution")
        return FlowControl.FAILURE

    # Choose execution mode based on Helper's threading decision
    if action_helper.use_threading:
        log.info("Using PARALLEL execution mode for {} actions", len(action_helper.actions))
        return run_parallel_state_machine(action_helper, context)
    else:
        log.info("Using SERIAL execution mode for {} actions", len(action_helper.actions))
        return run_serial_state_machine(action_helper, context)


def run_parallel_state_machine(action_helper: Helper, context: Any | None) -> FlowControl:
    """
    Execute state machine using parallel thread pool execution.

    Optimized for independent CloudFormation stacks and other long-running
    infrastructure operations that can benefit from concurrent execution.
    """
    log.trace("Entering parallel state machine execution")

    try:
        # Start thread pool executor
        action_helper.start_execution()

        # Main execution loop with threading
        max_iterations = 50  # Higher limit for threaded execution
        iteration = 0
        total_submitted = 0
        total_completed = 0

        while iteration < max_iterations and not timeout_imminent(context):
            iteration += 1
            log.debug("Parallel state machine iteration {} (max {})", iteration, max_iterations)

            # Get runnable actions BEFORE submitting to log properly
            runnable_actions_before = action_helper.get_runnable_actions()
            runnable_count_before = len(runnable_actions_before)

            # Phase 1: Submit runnable actions to thread pool
            submitted_count = action_helper.execute_parallel_actions(context)
            total_submitted += submitted_count

            if submitted_count > 0:
                if action_helper.is_rerun and runnable_actions_before:
                    rerun_actions = [a.name for a in runnable_actions_before[:submitted_count]]
                    log.info("RERUN: Submitting parallel actions: {}", rerun_actions)

                log.info("Submitted {} actions to thread pool", submitted_count)

            # Phase 2: Check for completed actions and execute hooks
            completed_count, failed_count = action_helper.check_completed_actions()
            total_completed += completed_count

            if completed_count > 0 or failed_count > 0:
                log.info("Iteration {}: completed={}, failed={}", iteration, completed_count, failed_count)

            # Phase 3: Check if execution is complete
            if action_helper.execution_complete():
                if action_helper.execution_successful():
                    log.info(
                        "All actions completed successfully in {} iterations (submitted={}, completed={})",
                        iteration,
                        total_submitted,
                        total_completed,
                    )

                    # Get final summary
                    final_summary = action_helper.get_execution_summary()
                    log.info("Final parallel execution summary: {}", final_summary)

                    return FlowControl.SUCCESS
                else:
                    log.error("Parallel execution completed with failures in {} iterations", iteration)

                    # Log failure details
                    failed_actions = action_helper.get_failed_actions()
                    log.error("Failed actions: {}", [a.name for a in failed_actions])

                    final_summary = action_helper.get_execution_summary()
                    log.error("Final parallel execution summary: {}", final_summary)

                    return FlowControl.FAILURE

            # Phase 4: Check for critical failures
            if action_helper.has_critical_failures():
                log.error("Critical failures detected in parallel execution, stopping")
                return FlowControl.FAILURE

            # Brief pause to allow threads to work and avoid tight loop
            if submitted_count == 0 and completed_count == 0:
                time.sleep(1.0)  # Longer pause when no work happening
            else:
                time.sleep(0.2)  # Short pause when work is active

        # Check why we exited the loop
        if iteration >= max_iterations:
            log.warning("Parallel execution reached max iterations ({}), continuing via Step Functions", max_iterations)
        if timeout_imminent(context):
            log.warning("Timeout imminent in parallel execution, continuing via Step Functions")

        return FlowControl.EXECUTE

    finally:
        # Always shutdown thread pool
        log.info("Shutting down thread pool")
        action_helper.shutdown(wait=True)


def run_serial_state_machine(action_helper: Helper, context: Any | None) -> FlowControl:
    """
    Execute state machine using traditional serial execution.

    Used for small deployments or when threading overhead isn't beneficial.
    """
    log.trace("Entering serial state machine execution")

    # Track progress for this iteration
    actions_processed = 0
    actions_executed = 0

    # Phase 1: Update status of currently running actions
    running_actions = action_helper.get_running_actions()
    log.debug("Checking status of {} running actions", len(running_actions))

    for action in running_actions:
        if timeout_imminent(context):
            log.warning("Timeout imminent, stopping running action status checks")
            break

        actions_processed += 1
        action_name = action.name

        try:
            log.trace("Checking status of running action: {}", action_name)

            # Check if action has completed or failed
            action.check()

            if action.is_complete():
                action_helper.update_action_status(action_name, ActionStatus.COMPLETE)
                log.info("Action {} completed successfully", action_name)

                # Execute lifecycle hooks for completed action
                execute_lifecycle_hooks(action_helper, action, "post_complete")

            elif action.is_failed():
                action_helper.update_action_status(action_name, ActionStatus.FAILED)
                log.error("Action {} failed during status check", action_name)

                # Execute lifecycle hooks for failed action
                execute_lifecycle_hooks(action_helper, action, "post_failure")

                # Check if this failure should stop execution
                if action_helper.has_critical_failures():
                    log.error("Critical action {} failed, stopping execution", action_name)
                    return FlowControl.FAILURE

            # If still running, status remains unchanged

        except Exception as e:
            log.error("Error checking status of action {}: {}", action_name, e)
            action_helper.update_action_status(action_name, ActionStatus.FAILED)

            # Check if this is a critical failure
            if action_helper.has_critical_failures():
                return FlowControl.FAILURE

    # Phase 2: Execute runnable actions with rerun awareness
    runnable_actions = action_helper.get_runnable_actions()
    log.debug("Found {} runnable actions", len(runnable_actions))

    # For reruns, log which actions are being re-executed
    if action_helper.is_rerun and runnable_actions:
        rerun_actions = [a.name for a in runnable_actions]
        log.info("RERUN: Re-executing actions: {}", rerun_actions)

    if runnable_actions:
        log.info(
            "Executing {} this iteration",
            _pluralize("runnable action", len(runnable_actions)),
            details={"RunnableActions": [a.name for a in runnable_actions]},
        )

    for action in runnable_actions:
        if timeout_imminent(context):
            log.warning("Timeout imminent, stopping new action execution")
            break

        actions_processed += 1
        actions_executed += 1
        action_name = action.name

        try:
            log.debug("Executing action: {}", action_name)

            # Mark action as running (this also adds to executed_this_run tracking)
            action_helper.update_action_status(action_name, ActionStatus.RUNNING)

            # Execute the action
            action.execute()

            # Check immediate completion status
            if action.is_complete():
                action_helper.update_action_status(action_name, ActionStatus.COMPLETE)
                log.info("Action {} completed immediately", action_name)

                # Execute lifecycle hooks for completed action
                execute_lifecycle_hooks(action_helper, action, "post_complete")

            elif action.is_failed():
                action_helper.update_action_status(action_name, ActionStatus.FAILED)
                log.error("Action {} failed during execution", action_name)

                # Execute lifecycle hooks for failed action
                execute_lifecycle_hooks(action_helper, action, "post_failure")

                # Check if this is a critical failure
                if action_helper.has_critical_failures():
                    log.error("Critical action {} failed, stopping execution", action_name)
                    return FlowControl.FAILURE

            # If action is still running, it will be checked in the next iteration

        except Exception as e:
            log.error("Error executing action {}: {}", action_name, e)
            action_helper.update_action_status(action_name, ActionStatus.FAILED)

            # Check if this is a critical failure
            if action_helper.has_critical_failures():
                return FlowControl.FAILURE

    # Log progress for this iteration
    log.debug(
        "Processed {} actions ({} executed, {} status checked) in this iteration",
        actions_processed,
        actions_executed,
        len(running_actions),
    )

    # Phase 3: Determine next state based on Helper's execution analysis
    if action_helper.execution_complete():
        if action_helper.execution_successful():
            log.info("All actions completed successfully")

            # Get final summary
            final_summary = action_helper.get_execution_summary()
            log.info("Final execution summary: {}", final_summary)

            return FlowControl.SUCCESS
        else:
            log.error("Execution completed with failures")

            # Log failure details
            failed_actions = action_helper.get_failed_actions()
            log.error("Failed actions: {}", [a.name for a in failed_actions])

            final_summary = action_helper.get_execution_summary()
            log.error("Final execution summary: {}", final_summary)

            return FlowControl.FAILURE
    else:
        # More work to do - determine if progress is possible
        pending_actions = action_helper.get_pending_actions()
        running_actions = action_helper.get_running_actions()

        pending_count = len(pending_actions)
        running_count = len(running_actions)

        # Check if we have runnable actions or running actions
        future_runnable = []
        for action in pending_actions:
            if action.name not in action_helper.executed_this_run:
                future_runnable.append(action)

        if running_count == 0 and len(future_runnable) == 0:
            # No running actions and no future runnable actions - execution is stuck
            log.error("No runnable actions remaining, execution stuck")
            log.error("Pending actions: {}", [a.name for a in pending_actions])
            return FlowControl.FAILURE
        else:
            # Continue execution - we have work to do
            log.info(
                "Continuing execution ({} pending, {} running, {} future runnable)",
                pending_count,
                running_count,
                len(future_runnable),
            )

            if len(future_runnable) > 0:
                log.debug("Future runnable actions: {}", [a.name for a in future_runnable])

            return FlowControl.EXECUTE


def execute_lifecycle_hooks(action_helper: Helper, parent_action: BaseAction, hook_type: str = "post_complete"):
    """
    Execute lifecycle hooks for a completed or failed action.

    Processes the lifecycle_hooks defined in the parent action's ActionResource,
    creating and executing them as additional actions. Lifecycle hooks are useful
    for notifications, cleanup, or dependent actions that should run after the
    main action completes.

    :param action_helper: Helper managing action execution
    :type action_helper: Helper
    :param parent_action: Action that triggered the lifecycle hooks
    :type parent_action: BaseAction
    :param hook_type: Type of lifecycle event (post_complete, post_failure)
    :type hook_type: str
    """
    if not parent_action.definition.lifecycle_hooks:
        log.trace("No lifecycle hooks defined for action {}", parent_action.name)
        return

    num_hooks = len(parent_action.definition.lifecycle_hooks)
    log.info("Executing {} lifecycle hooks for action {} ({})", num_hooks, parent_action.name, hook_type)

    for i, hook_action_resource in enumerate(parent_action.definition.lifecycle_hooks):
        try:
            # Create unique hook name with parent namespace inheritance
            hook_base_name = hook_action_resource.action_name
            hook_name = f"{parent_action.name}/{hook_base_name}"
            log.debug("Processing lifecycle hook {}/{}: {}", i + 1, num_hooks, hook_name)

            # Check if hook action already exists in helper
            if hook_name in action_helper.action_instances:
                hook_action = action_helper.action_instances[hook_name]
                log.trace("Using existing hook action instance: {}", hook_name)
            else:
                # Create new hook action instance
                log.trace("Creating new hook action instance: {}", hook_name)
                hook_action = action_helper._create_action_instance(
                    hook_action_resource, 
                    parent_action_name=parent_action.name
                )
                action_helper.action_instances[hook_name] = hook_action
                action_helper.action_status[hook_name] = ActionStatus.PENDING

            # Execute hook if it hasn't been executed this run and is in pending state
            current_status = action_helper.action_status.get(hook_name, ActionStatus.PENDING)

            if current_status == ActionStatus.PENDING and hook_name not in action_helper.executed_this_run:

                log.info("Executing lifecycle hook: {} (parent: {})", hook_name, parent_action.name)

                # Mark as running and executed this run
                action_helper.update_action_status(hook_name, ActionStatus.RUNNING)

                try:
                    # Execute the lifecycle hook
                    hook_action.execute()

                    # Check completion status
                    if hook_action.is_complete():
                        action_helper.update_action_status(hook_name, ActionStatus.COMPLETE)
                        log.info("Lifecycle hook {} completed successfully", hook_name)
                    elif hook_action.is_failed():
                        action_helper.update_action_status(hook_name, ActionStatus.FAILED)
                        log.warning("Lifecycle hook {} failed (non-critical)", hook_name)
                    else:
                        # Hook is still running - will be checked in future iterations
                        log.debug("Lifecycle hook {} is still running", hook_name)

                except Exception as hook_error:
                    log.error("Error executing lifecycle hook {}: {}", hook_name, hook_error)
                    action_helper.update_action_status(hook_name, ActionStatus.FAILED)

            else:
                log.debug(
                    "Skipping lifecycle hook {} - status: {}, executed_this_run: {}",
                    hook_name,
                    current_status,
                    hook_name in action_helper.executed_this_run,
                )

        except Exception as e:
            log.error("Error processing lifecycle hook for {}: {}", parent_action.name, e)
            # Continue with other hooks even if one fails


def _pluralize(phrase: str, count: int) -> str:
    """Generate properly pluralized phrase with count.

    :param phrase: Base phrase to pluralize
    :type phrase: str
    :param count: Count for pluralization
    :type count: int
    :return: Formatted phrase with count and proper pluralization
    :rtype: str
    """
    return f"{count} {_p.plural(phrase, count)}"


def _percentage(numerator: int, denominator: int) -> str:
    """Calculate percentage as formatted string.

    :param numerator: Top value
    :type numerator: int
    :param denominator: Bottom value
    :type denominator: int
    :return: Percentage string with % symbol
    :rtype: str
    """
    if denominator == 0:
        return "100%"
    else:
        return "{}%".format(int(float(numerator) / float(denominator) * 100.0))


# S3 Operations - Load and Save Functions


def load_actions(task_payload: TaskPayload) -> list[ActionResource]:
    """
    Load ActionResource definitions from S3 or embedded package.

    Downloads the actions file from S3 and parses it based on the content type.
    Supports both YAML and JSON formats. If actions are embedded in the task
    payload package, those are used directly.

    :param task_payload: The TaskPayload object containing actions details
    :type task_payload: TaskPayload
    :return: List of ActionResource objects loaded from S3 or package
    :rtype: list[ActionResource]
    :raises ValueError: If no actions found in task payload or unknown content type
    :raises Exception: If S3 operation fails or data parsing fails
    """
    log.trace("Loading actions")

    # Check if actions are embedded in the package first (takes priority)
    if task_payload.package and task_payload.package.actions:
        log.debug(
            "Using {} actions from embedded package",
            len(task_payload.package.actions),
        )
        return task_payload.package.actions

    # Load actions from S3
    actions_details = task_payload.actions
    if actions_details is None:
        raise ValueError("No actions found in the task payload")

    bucket_name = actions_details.bucket_name
    bucket_region = actions_details.bucket_region

    log.info("Downloading actions from {}", actions_details.key)

    try:
        s3_client = MagicS3Client.get_client(Region=bucket_region)

        actions_fileobj = io.BytesIO()
        download_details: dict = s3_client.download_fileobj(Bucket=bucket_name, Key=actions_details.key, Fileobj=actions_fileobj)

        content_type = download_details.get("ContentType", "application/x-yaml")
        version_id = download_details.get("VersionId", None)

        log.debug(
            "Actions download successful",
            details={
                "bucket": bucket_name,
                "key": actions_details.key,
                "version_id": version_id,
                "content_type": content_type,
            },
        )

    except Exception as e:
        log.error(
            "Failed to download actions from S3 bucket {} key {}: {}",
            bucket_name,
            actions_details.key,
            e,
        )
        raise Exception(f"Failed to load actions from S3: {str(e)}") from e

    try:
        if util.is_yaml_mimetype(content_type):
            actions_data = util.read_yaml(actions_fileobj)
        elif util.is_json_mimetype(content_type):
            actions_data = util.read_json(actions_fileobj)
        else:
            raise ValueError(f"Actions file unknown content type: {content_type}")

        # Update actions details with content type
        actions_details.content_type = content_type

        log.debug("Loaded Actions Content Type: {}", content_type)
        log.debug("Loaded Actions Data: ", details=actions_data)

        if actions_data is None:
            log.trace("Actions file was empty or null, returning empty list")
            return []

        # Convert to ActionResource objects
        actions: list[ActionResource] = [ActionResource(**action) for action in actions_data]

        log.trace("Actions loaded successfully ({} actions)", len(actions))
        return actions

    except Exception as e:
        log.error("Failed to parse actions data with content type {}: {}", content_type, e)
        raise Exception(f"Failed to parse actions data: {str(e)}") from e


def load_state(task_payload: TaskPayload) -> dict:
    """
    Load the execution state from S3.

    Downloads and parses the state file from S3. The state data is a dictionary
    containing facts and execution state information. Supports both YAML and JSON
    formats based on the content type. Handles new state creation gracefully.

    :param task_payload: The TaskPayload object containing state details
    :type task_payload: TaskPayload
    :return: Dictionary containing the execution state data
    :rtype: dict
    :raises ValueError: If no state found in task payload
    :raises Exception: If state file has unknown content type or S3 operation fails
    """
    log.trace("Loading state")

    state_details = task_payload.state
    if state_details is None:
        raise ValueError("No state found in the task payload")

    # Handle new state creation
    if state_details.version_id == "new":
        log.info("Creating new state (no existing state file)")
        return {}

    # Prepare S3 request parameters
    extra_args = {}
    if state_details.version_id is not None:
        extra_args["VersionId"] = state_details.version_id

    log.info("Loading state from {}", state_details.get_full_path())

    try:
        # Retrieve state from S3 (or magic bucket for local development)
        s3_client = MagicS3Client.get_client(Region=state_details.bucket_region)

        state_fileobj = io.BytesIO()
        state_download_response = s3_client.download_fileobj(
            Bucket=state_details.bucket_name,
            Key=state_details.key,
            Fileobj=state_fileobj,
            ExtraArgs=extra_args,
        )

        content_type = state_download_response.get("ContentType", "application/x-yaml")
        state_details.content_type = content_type
        version_id = state_download_response.get("VersionId", None)
        state_details.version_id = version_id

        log.debug(
            "State download successful",
            details={
                "bucket": state_details.bucket_name,
                "key": state_details.key,
                "version_id": version_id,
                "content_type": content_type,
            },
        )

    except Exception as e:
        log.error(
            "Failed to download state from S3 bucket {} key {} version {}: {}",
            state_details.bucket_name,
            state_details.key,
            state_details.version_id,
            e,
        )
        raise Exception(f"Failed to load state from S3: {str(e)}") from e

    try:
        # Parse state data based on content type
        if util.is_yaml_mimetype(content_type):
            state = util.read_yaml(state_fileobj)
        elif util.is_json_mimetype(content_type):
            state = util.read_json(state_fileobj)
        else:
            raise Exception(f"State file unknown content type: {content_type}")

        log.debug("Loaded State Content Type: {}", content_type)
        log.debug("Loaded State Data: ", details=state)

        if state is None:
            log.trace("State file was empty or null, returning empty dict")
            return {}

        log.trace("State loaded successfully ({} keys)", len(state.keys()) if state else 0)
        return state

    except Exception as e:
        log.error("Failed to parse state data with content type {}: {}", content_type, e)
        raise Exception(f"Failed to parse state data: {str(e)}") from e


def save_state(task_payload: TaskPayload, state: dict) -> None:
    """
    Save the execution state to S3.

    Serializes the state dictionary and saves it to S3. The format (YAML or JSON)
    is determined by the content type in the state details. Updates the version_id
    in the task payload with the new S3 object version for tracking.

    :param task_payload: The TaskPayload object containing state details
    :type task_payload: TaskPayload
    :param state: Dictionary containing the execution state to save
    :type state: dict
    :raises ValueError: If no state found in task payload
    :raises Exception: If S3 operation fails or data serialization fails
    """
    log.trace("Saving state")

    state_details = task_payload.state
    if state_details is None:
        raise ValueError("No state found in the task payload")

    content_type = state_details.content_type or "application/x-yaml"

    log.debug("Saving State Content Type: {}", content_type)
    log.debug("Saving State Data: ", details=state)

    try:
        # Serialize state data based on content type
        if util.is_yaml_mimetype(content_type):
            result_data = util.to_yaml(state)
        elif util.is_json_mimetype(content_type):
            result_data = util.to_json(state)
        else:
            raise ValueError(f"Unsupported content type for state serialization: {content_type}")

    except Exception as e:
        log.error("Failed to serialize state data with content type {}: {}", content_type, e)
        raise Exception(f"Failed to serialize state data: {str(e)}") from e

    log.info("Saving state to {}", state_details.key)

    try:
        s3_client = MagicS3Client.get_client(Region=state_details.bucket_region)

        response = s3_client.put_object(
            Bucket=state_details.bucket_name,
            Key=state_details.key,
            Body=result_data,
            ContentType=content_type,
            ServerSideEncryption="AES256",
        )

        log.debug("State save response: ", details=response)

        # Update version ID for future references
        state_details.version_id = response.version_id

        log.trace("State saved successfully to S3 (version: {})", response.version_id)

    except Exception as e:
        log.error(
            "Failed to save state to S3 bucket {} key {}: {}",
            state_details.bucket_name,
            state_details.key,
            e,
        )
        raise Exception(f"Failed to save state to S3: {str(e)}") from e

    log.trace("State save complete")


def save_actions(task_payload: TaskPayload, actions: list[ActionResource]) -> None:
    """
    Save ActionResource definitions to S3.

    Serializes the list of ActionResource objects and saves them to S3 as YAML format.
    Updates the version_id in the task payload with the new S3 object version.
    Used primarily for debugging and action modification workflows.

    :param task_payload: The TaskPayload object containing actions details
    :type task_payload: TaskPayload
    :param actions: List of ActionResource objects to save
    :type actions: list[ActionResource]
    :raises ValueError: If no actions file definition found in task payload
    :raises TypeError: If actions contains non-ActionResource objects
    :raises Exception: If S3 operation fails or data serialization fails
    """
    actions_details = task_payload.actions
    if not actions_details:
        raise ValueError("No actions file definition found in the task payload")

    # Convert ActionResource objects to dictionaries
    data: list[dict] = []
    for action in actions:
        if isinstance(action, ActionResource):
            data.append(action.model_dump())
        else:
            raise TypeError(f"Expected ActionResource, got {type(action)}")

    content_type = actions_details.content_type or "application/x-yaml"

    log.debug("Saving Actions Content Type: {}", content_type)
    log.debug("Saving Actions Data: ", details={"Actions": data})

    try:
        # Serialize the data to YAML format
        serialized_data = util.to_yaml(data)
        log.debug("Actions data serialized successfully")

    except Exception as e:
        log.error("Failed to serialize actions data to YAML: {}", e)
        raise Exception(f"Failed to serialize actions data: {str(e)}") from e

    try:
        s3_client = MagicS3Client.get_client(Region=actions_details.bucket_region, DataPath=actions_details.data_path)

        response = s3_client.put_object(
            Bucket=actions_details.bucket_name,
            Key=actions_details.key,
            Body=serialized_data,
            ContentType=actions_details.content_type,
            ServerSideEncryption="AES256",
        )

        log.debug("Actions save response: ", details=response)

        # Update version ID for future references
        actions_details.version_id = response.version_id

        log.trace("Actions saved successfully to S3 (version: {})", response.version_id)

    except Exception as e:
        log.error(
            "Failed to save actions to S3 bucket {} key {}: {}",
            actions_details.bucket_name,
            actions_details.key,
            e,
        )
        raise Exception(f"Failed to save actions to S3: {str(e)}") from e

    log.trace("Actions save complete")
