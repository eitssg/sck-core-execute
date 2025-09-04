from typing import Dict, List, Set, Optional, Tuple
from enum import Enum
import threading
from concurrent.futures import ThreadPoolExecutor, Future

import core_logging as log

from core_framework.models import ActionResource, TaskPayload

from .action import BaseAction
from .factory import create_action


class ActionStatus(str, Enum):
    """Status of individual actions."""

    PENDING = "pending"  # Not yet started
    RUNNING = "running"  # Currently executing
    COMPLETE = "complete"  # Successfully completed
    FAILED = "failed"  # Failed execution
    SKIPPED = "skipped"  # Skipped due to conditions
    BLOCKED = "blocked"  # Dependencies not met


class FlowControl(str, Enum):
    """Flow control for task execution."""

    INIT = "init"  # Initial state - can re-run everything
    EXECUTE = "execute"  # Normal execution mode
    SUCCESS = "success"  # All actions completed successfully
    FAILURE = "failure"  # Critical failures that stop execution


class Helper:
    """Multi-threaded helper for managing parallel action execution with re-run capabilities and dependency management."""

    def __init__(self, actions: List[ActionResource], context_state: dict, task_payload: TaskPayload):
        """Initialize helper with actions and state."""
        self.actions = actions
        self.context_state = context_state
        self.task_payload = task_payload
        self.action_instances: Dict[str, BaseAction] = {}
        self.action_status: Dict[str, ActionStatus] = {}
        self.dependency_graph = self._build_dependency_graph()

        # Thread pool configuration
        self.max_workers = self._calculate_optimal_workers()
        self.executor: Optional[ThreadPoolExecutor] = None
        self.running_futures: Dict[str, Future] = {}
        self.action_lock = threading.RLock()  # Protect shared state
        self.use_threading = self._should_use_threading()

        # Determine if this is a re-run based on flow_control
        self.is_rerun = task_payload.flow_control == FlowControl.INIT.value

        # Track actions executed this Step Function run
        self.executed_this_run: Set[str] = set()

        log.info(
            "Helper initialized with {} actions (rerun={}, threading={}, max_workers={})",
            len(actions),
            self.is_rerun,
            self.use_threading,
            self.max_workers,
        )
        self._initialize_actions()

    def _calculate_optimal_workers(self) -> int:
        """Calculate optimal number of worker threads based on action count and types."""
        action_count = len(self.actions)

        # Conservative scaling: 1 thread per 3-5 actions, max 10
        if action_count <= 5:
            return min(action_count, 2)  # Small deployments: 1-2 threads
        elif action_count <= 20:
            return min(action_count // 3, 5)  # Medium deployments: 3-5 threads
        else:
            return min(action_count // 4, 10)  # Large deployments: max 10 threads

    def _should_use_threading(self) -> bool:
        """Determine if threading should be used based on action characteristics."""
        # Use threading if we have more than 3 actions
        if len(self.actions) < 3:
            return False

        # Check for independent actions (no dependencies)
        independent_actions = 0
        for action in self.actions:
            deps = action.depends_on or []
            after_deps = action.after or []
            if not deps and not after_deps:
                independent_actions += 1

        # Use threading if we have multiple independent actions
        return independent_actions >= 2

    def _build_dependency_graph(self) -> Dict[str, Set[str]]:
        """Build dependency graph from actions."""
        graph = {}
        for action in self.actions:
            action_name = action.action_name
            dependencies = set()

            # Add explicit dependencies
            if action.depends_on:
                dependencies.update(action.depends_on)

            # Add after dependencies
            if action.after:
                dependencies.update(action.after)

            graph[action_name] = dependencies
        return graph

    def _initialize_actions(self):
        """Enhanced action initialization with better rerun handling."""
        for action_resource in self.actions:
            action_name = action_resource.action_name

            try:
                # Create action instance
                action_instance = self._create_action_instance(action_resource)

                with self.action_lock:
                    self.action_instances[action_name] = action_instance

                    # Determine initial status
                    if self.is_rerun:
                        # RERUN: Check if action can be reinitialized
                        if hasattr(action_instance, "can_initialize") and action_instance.can_initialize():
                            log.info("Reinitializing action {} for rerun", action_name)

                            if hasattr(action_instance, "initialize"):
                                action_instance.initialize()

                            # Set status based on whether action was previously completed
                            existing_status = self._get_existing_status(action_name)
                            if existing_status in [ActionStatus.COMPLETE, ActionStatus.FAILED]:
                                # Reset to pending for rerun
                                self.action_status[action_name] = ActionStatus.PENDING
                                log.info("Reset action {} from {} to PENDING for rerun", action_name, existing_status.value)
                            else:
                                self.action_status[action_name] = existing_status

                            # Clear from executed tracking to allow re-execution
                            self.executed_this_run.discard(action_name)

                        else:
                            log.warning("Action {} cannot be reinitialized, keeping existing status", action_name)
                            existing_status = self._get_existing_status(action_name)
                            self.action_status[action_name] = existing_status

                            # If it was complete/failed and can't reinitialize, don't re-execute
                            if existing_status in [ActionStatus.COMPLETE, ActionStatus.FAILED]:
                                self.executed_this_run.add(action_name)
                    else:
                        # NORMAL: Use existing status from state
                        existing_status = self._get_existing_status(action_name)
                        self.action_status[action_name] = existing_status

                        # Track already completed actions to prevent re-execution
                        if existing_status in [ActionStatus.COMPLETE, ActionStatus.FAILED]:
                            self.executed_this_run.add(action_name)

                log.debug("Action {} initialized with status: {}", action_name, self.action_status[action_name])

            except Exception as e:
                log.error("Failed to initialize action {}: {}", action_name, e)
                with self.action_lock:
                    self.action_status[action_name] = ActionStatus.FAILED

    def start_execution(self) -> ThreadPoolExecutor:
        """Start the thread pool executor for parallel action execution."""
        if not self.use_threading:
            log.debug("Threading disabled, skipping thread pool creation")
            return None

        if self.executor is not None:
            log.warning("Thread pool already started, shutting down previous instance")
            self.shutdown()

        log.info("Starting thread pool with {} workers", self.max_workers)
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="action-worker")
        return self.executor

    def execute_parallel_actions(self, timeout_context=None) -> int:
        """Execute actions in parallel using thread pool.

        Returns:
            Number of actions submitted for execution
        """
        if not self.use_threading:
            return 0  # Caller should use serial execution

        if self.executor is None:
            self.start_execution()

        actions_submitted = 0

        # Submit all runnable actions to thread pool
        runnable_actions = self.get_runnable_actions()

        for action in runnable_actions:
            if self._timeout_imminent(timeout_context):
                log.warning("Timeout imminent, stopping action submission")
                break

            action_name = action.name

            # Submit action to thread pool
            future = self.executor.submit(self._execute_action_wrapper, action)

            with self.action_lock:
                self.running_futures[action_name] = future
                self.action_status[action_name] = ActionStatus.RUNNING
                self.executed_this_run.add(action_name)

            actions_submitted += 1
            log.debug("Submitted action {} to thread pool (worker thread)", action_name)

        log.info("Submitted {} actions to thread pool", actions_submitted)
        return actions_submitted

    def check_completed_actions(self) -> Tuple[int, int]:
        """Check for completed actions and execute their lifecycle hooks.

        Returns:
            Tuple of (completed_count, failed_count)
        """
        if not self.use_threading:
            return 0, 0  # Serial execution handles this differently

        completed_count = 0
        failed_count = 0
        completed_futures = []

        with self.action_lock:
            # Check all running futures for completion
            for action_name, future in list(self.running_futures.items()):
                if future.done():
                    completed_futures.append((action_name, future))

        # Process completed futures outside the lock
        for action_name, future in completed_futures:
            try:
                # Get the result (this will raise exception if action failed)
                result = future.result()
                action = self.action_instances[action_name]

                if action.is_complete():
                    with self.action_lock:
                        self.action_status[action_name] = ActionStatus.COMPLETE
                        del self.running_futures[action_name]

                    completed_count += 1
                    log.info("Action {} completed successfully in thread pool", action_name)

                    # Execute lifecycle hooks in main thread (not thread pool)
                    self._execute_lifecycle_hooks_sync(action, "post_complete")

                elif action.is_failed():
                    with self.action_lock:
                        self.action_status[action_name] = ActionStatus.FAILED
                        del self.running_futures[action_name]

                    failed_count += 1
                    log.error("Action {} failed in thread pool", action_name)

                    # Execute lifecycle hooks for failed action
                    self._execute_lifecycle_hooks_sync(action, "post_failure")

            except Exception as e:
                # Action raised an exception
                with self.action_lock:
                    self.action_status[action_name] = ActionStatus.FAILED
                    del self.running_futures[action_name]

                failed_count += 1
                log.error("Action {} raised exception in thread pool: {}", action_name, e)

        return completed_count, failed_count

    def _execute_action_wrapper(self, action: BaseAction) -> bool:
        """Thread-safe wrapper for action execution.

        This runs in a worker thread.
        """
        try:
            # Set thread-local logging context
            log.set_identity(action.name)
            log.debug("Executing action {} in worker thread {}", action.name, threading.current_thread().name)

            # Execute the action
            action.execute()

            log.debug("Action {} execution completed in worker thread", action.name)
            return True

        except Exception as e:
            log.error("Action {} failed in worker thread: {}", action.name, e)
            # Set failed status will be handled by check_completed_actions
            return False
        finally:
            log.reset_identity()

    def _execute_lifecycle_hooks_sync(self, parent_action: BaseAction, hook_type: str):
        """Execute lifecycle hooks synchronously in main thread."""
        if not parent_action.definition.lifecycle_hooks:
            return

        log.info(
            "Executing {} lifecycle hooks for action {} ({})",
            len(parent_action.definition.lifecycle_hooks),
            parent_action.name,
            hook_type,
        )

        for hook_action_resource in parent_action.definition.lifecycle_hooks:
            try:
                # Create unique hook name with parent namespace inheritance
                hook_base_name = hook_action_resource.action_name
                hook_name = f"{parent_action.name}/{hook_base_name}"

                # Create hook action if not exists
                if hook_name not in self.action_instances:
                    # Create lifecycle hook with parent context for namespace inheritance
                    hook_action = self._create_action_instance(hook_action_resource, parent_action_name=parent_action.name)
                    with self.action_lock:
                        self.action_instances[hook_name] = hook_action
                        self.action_status[hook_name] = ActionStatus.PENDING

                # Execute hook if not already executed this run
                with self.action_lock:
                    current_status = self.action_status.get(hook_name, ActionStatus.PENDING)
                    already_executed = hook_name in self.executed_this_run

                if current_status == ActionStatus.PENDING and not already_executed:
                    log.info("Executing lifecycle hook: {} (parent: {})", hook_name, parent_action.name)

                    hook_action = self.action_instances[hook_name]

                    with self.action_lock:
                        self.action_status[hook_name] = ActionStatus.RUNNING
                        self.executed_this_run.add(hook_name)

                    # Execute hook synchronously in main thread
                    hook_action.execute()

                    if hook_action.is_complete():
                        with self.action_lock:
                            self.action_status[hook_name] = ActionStatus.COMPLETE
                        log.info("Lifecycle hook {} completed", hook_name)
                    elif hook_action.is_failed():
                        with self.action_lock:
                            self.action_status[hook_name] = ActionStatus.FAILED
                        log.warning("Lifecycle hook {} failed (non-critical)", hook_name)

            except Exception as e:
                log.error("Error executing lifecycle hook for {}: {}", parent_action.name, e)

    def _create_action_instance(self, action_resource: ActionResource, parent_action_name: str = None) -> BaseAction:
        """Create action instance from ActionResource."""

        return create_action(
            action_resource, self.context_state, self.task_payload.deployment_details, parent_action_name=parent_action_name
        )

    def _get_existing_status(self, action_name: str) -> ActionStatus:
        """Get existing status from state context."""
        status_key = f"{action_name}/StatusCode"
        existing_status = self.context_state.get(status_key)

        if existing_status == "complete":
            return ActionStatus.COMPLETE
        elif existing_status == "failed":
            return ActionStatus.FAILED
        elif existing_status == "running":
            return ActionStatus.RUNNING
        else:
            return ActionStatus.PENDING

    def get_runnable_actions(self) -> List[BaseAction]:
        """Get actions that can be executed right now."""
        runnable = []

        with self.action_lock:
            for action_name, action_instance in self.action_instances.items():
                status = self.action_status[action_name]

                # Skip if not in pending state
                if status != ActionStatus.PENDING:
                    continue

                # Skip if already executed this Step Function run
                if action_name in self.executed_this_run:
                    log.debug("Action {} already executed this Step Function run, skipping", action_name)
                    continue

                # Check if dependencies are met
                if self._dependencies_satisfied(action_name):
                    # Check action-specific conditions
                    if self._action_can_run(action_instance):
                        runnable.append(action_instance)
                    else:
                        self.action_status[action_name] = ActionStatus.BLOCKED

        log.debug("Found {} runnable actions", len(runnable))
        return runnable

    def get_running_actions(self) -> List[BaseAction]:
        """Get actions that are currently running."""
        running = []
        with self.action_lock:
            for action_name, action_instance in self.action_instances.items():
                if self.action_status[action_name] == ActionStatus.RUNNING:
                    running.append(action_instance)
        return running

    def get_failed_actions(self) -> List[BaseAction]:
        """Get actions that have failed."""
        failed = []
        with self.action_lock:
            for action_name, action_instance in self.action_instances.items():
                if self.action_status[action_name] == ActionStatus.FAILED:
                    failed.append(action_instance)
        return failed

    def get_completed_actions(self) -> List[BaseAction]:
        """Get actions that have completed successfully."""
        completed = []
        with self.action_lock:
            for action_name, action_instance in self.action_instances.items():
                if self.action_status[action_name] == ActionStatus.COMPLETE:
                    completed.append(action_instance)
        return completed

    def get_pending_actions(self) -> List[BaseAction]:
        """Get actions that are still pending."""
        pending = []
        with self.action_lock:
            for action_name, action_instance in self.action_instances.items():
                status = self.action_status[action_name]
                if status in [ActionStatus.PENDING, ActionStatus.BLOCKED]:
                    pending.append(action_instance)
        return pending

    def update_action_status(self, action_name: str, new_status: ActionStatus):
        """Update the status of an action and execute lifecycle hooks."""
        with self.action_lock:
            old_status = self.action_status.get(action_name)
            self.action_status[action_name] = new_status

            log.debug("Action {} status: {} -> {}", action_name, old_status, new_status)

            # Track when actions start executing
            if new_status == ActionStatus.RUNNING:
                self.executed_this_run.add(action_name)
                log.debug("Marked action {} as executed this Step Function run", action_name)

            # Update context state for persistence
            self.context_state[f"{action_name}/StatusCode"] = new_status.value

        # Execute lifecycle hooks when action completes (outside lock to avoid deadlock)
        if (
            new_status == ActionStatus.COMPLETE and old_status != ActionStatus.COMPLETE and not self.use_threading
        ):  # Only for serial execution - parallel handles hooks differently
            action_instance = self.action_instances.get(action_name)
            if action_instance:
                self._execute_lifecycle_hooks_sync(action_instance, "post_complete")

    def _dependencies_satisfied(self, action_name: str) -> bool:
        """Check if all dependencies for an action are satisfied."""
        dependencies = self.dependency_graph.get(action_name, set())

        for dep_name in dependencies:
            dep_status = self.action_status.get(dep_name)
            if dep_status != ActionStatus.COMPLETE:
                log.trace("Action {} blocked by dependency {} (status: {})", action_name, dep_name, dep_status)
                return False

        return True

    def _action_can_run(self, action_instance: BaseAction) -> bool:
        """Check if action can run based on conditions and constraints."""
        try:
            # Check if action has the can_execute method, otherwise assume it can run
            if hasattr(action_instance, "can_execute"):
                return action_instance.can_execute()
            else:
                log.warning("Action {} missing can_execute method, assuming it can run", action_instance.name)
                return True
        except Exception as e:
            log.error("Error checking if action can run: {}", e)
            return False

    def has_critical_failures(self) -> bool:
        """Check if there are critical failures that should stop execution."""
        failed_actions = self.get_failed_actions()

        # Define your critical failure logic here
        # For example, certain action types might be critical
        for action in failed_actions:
            if self._is_critical_action(action):
                log.error("Critical action {} failed, stopping execution", action.name)
                return True

        return False

    def _is_critical_action(self, action: BaseAction) -> bool:
        """Determine if an action is critical for overall success."""
        # Define your critical action logic
        # Example: IAM role creation might be critical
        critical_kinds = ["AWS::IAM::Role", "SYSTEM::Authentication"]
        return action.definition.kind in critical_kinds

    def execution_complete(self) -> bool:
        """Check if execution is complete (all actions in final state)."""
        with self.action_lock:
            # Check if any futures are still running
            if self.running_futures:
                return False

            # Check action statuses
            for action_name, status in self.action_status.items():
                if status in [ActionStatus.PENDING, ActionStatus.RUNNING, ActionStatus.BLOCKED]:
                    # Check if this action could still run
                    if action_name not in self.executed_this_run:
                        return False
        return True

    def execution_successful(self) -> bool:
        """Check if execution was successful (no failed actions or only non-critical failures)."""
        if not self.execution_complete():
            return False

        failed_actions = self.get_failed_actions()
        if not failed_actions:
            return True

        # Check if all failures are non-critical
        return not self.has_critical_failures()

    def shutdown(self, wait: bool = True):
        """Shutdown the thread pool executor."""
        if self.executor is not None:
            log.info("Shutting down thread pool (wait={})", wait)
            self.executor.shutdown(wait=wait)
            self.executor = None

            with self.action_lock:
                # Cancel any remaining futures
                for future in self.running_futures.values():
                    future.cancel()
                self.running_futures.clear()

    def _timeout_imminent(self, context) -> bool:
        """Check if timeout is imminent."""
        from ..execute import timeout_imminent

        return timeout_imminent(context)

    def get_execution_summary(self) -> dict:
        """Get summary of execution state."""
        status_counts = {}
        for status in ActionStatus:
            status_counts[status.value] = sum(1 for s in self.action_status.values() if s == status)

        return {
            "total_actions": len(self.actions),
            "status_counts": status_counts,
            "is_rerun": self.is_rerun,
            "use_threading": self.use_threading,
            "max_workers": self.max_workers,
            "running_futures": len(self.running_futures) if self.running_futures else 0,
            "complete": self.execution_complete(),
            "successful": self.execution_successful(),
            "has_critical_failures": self.has_critical_failures(),
        }

    # Legacy method compatibility (for existing execute.py)
    def runnable_actions(self) -> List[BaseAction]:
        """Legacy method name for get_runnable_actions."""
        return self.get_runnable_actions()

    def running_actions(self) -> List[BaseAction]:
        """Legacy method name for get_running_actions."""
        return self.get_running_actions()

    def failed_actions(self) -> List[BaseAction]:
        """Legacy method name for get_failed_actions."""
        return self.get_failed_actions()

    def completed_actions(self) -> List[BaseAction]:
        """Legacy method name for get_completed_actions."""
        return self.get_completed_actions()

    def pending_actions(self) -> List[BaseAction]:
        """Legacy method name for get_pending_actions."""
        return self.get_pending_actions()

    def incomplete_actions(self) -> List[BaseAction]:
        """Get actions that are not complete (pending, running, blocked, failed)."""
        incomplete = []
        with self.action_lock:
            for action_name, action_instance in self.action_instances.items():
                status = self.action_status[action_name]
                if status != ActionStatus.COMPLETE:
                    incomplete.append(action_instance)
        return incomplete

    def rerunnable_actions(self) -> List[BaseAction]:
        """Get actions that can be re-run (have is_rerunnable() == True and are complete/failed)."""
        rerunnable = []
        with self.action_lock:
            for action_name, action_instance in self.action_instances.items():
                status = self.action_status[action_name]
                if (
                    status in [ActionStatus.COMPLETE, ActionStatus.FAILED]
                    and hasattr(action_instance, "is_rerunnable")
                    and action_instance.is_rerunnable()
                ):
                    rerunnable.append(action_instance)
        return rerunnable
