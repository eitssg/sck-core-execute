from typing import Dict, List, Optional
import threading
from concurrent.futures import ThreadPoolExecutor, Future

import core_logging as log

from core_framework.models import ActionResource, TaskPayload

from .action import StatusCode, BaseAction
from .factory import create_action


class Helper:
    """Multi-threaded helper for managing parallel action execution with re-run capabilities and dependency management."""

    def __init__(self, resources: List[ActionResource], context: dict, task_payload: TaskPayload):
        """Initialize helper with actions and state."""
        self.number_running: int = 0
        self.number_pending: int = 0
        self.number_complete: int = 0
        self.number_failed: int = 0

        self.actions_resources: List[ActionResource] = resources
        self.context_state: dict = context
        self.task_payload: TaskPayload = task_payload
        self.actions: Dict[str, BaseAction] = {}

        # Thread pool configuration
        self.use_threading = self._should_use_threading(resources)
        self.max_workers = self._calculate_optimal_workers(resources)
        self.executor: Optional[ThreadPoolExecutor] = (
            ThreadPoolExecutor(max_workers=self.max_workers) if self.max_workers > 1 else None
        )
        self.running_futures: Dict[str, Future] = {}
        self.slots = threading.BoundedSemaphore(self.max_workers if self.max_workers > 1 else 1)
        self.action_lock = threading.RLock()

        # Determine if this is the first loop through the state machine, so we should initialize all actions
        self.initialize = task_payload.flow_control is None or task_payload.flow_control == "init"

        log.debug(
            "Helper initialized with {} actions (initialize={}, threading={}, max_workers={})",
            len(self.actions),
            self.initialize,
            self.use_threading,
            self.max_workers,
        )

        # Calling a class method within __init__ is generally not recommended, it's really bad practice.
        self._load_actions(resources)

    def _load_actions(self, resources: List[ActionResource]) -> Dict[str, BaseAction]:

        for action_resource in resources:
            try:

                action: BaseAction = create_action(
                    action_resource,
                    self.context_state,
                    self.task_payload.deployment_details,
                )
                self.actions[action_resource.action_key] = action
                self._initialize_actions(action)

            except Exception as e:
                log.error("Failed to load action {}: {}", action_resource.action_name, e)

    @staticmethod
    def _calculate_optimal_workers(action_resources: List[ActionResource]) -> int:
        """Calculate optimal number of worker threads based on action count and types."""
        action_count = len(action_resources)

        # Conservative scaling: 1 thread per 3-5 actions, max 10
        if action_count <= 5:
            return min(action_count, 2)  # Small deployments: 1-2 threads
        elif action_count <= 20:
            return min(action_count // 3, 5)  # Medium deployments: 3-5 threads
        else:
            return min(action_count // 4, 10)  # Large deployments: max 10 threads

    @staticmethod
    def _should_use_threading(action_resources: List[ActionResource]) -> bool:
        """Determine if threading should be used based on action characteristics."""
        # Use threading if we have more than 3 actions
        if len(action_resources) < 3:
            return False

        # Check for independent actions (no dependencies)
        independent_actions = 0
        for action_resource in action_resources:
            deps = action_resource.depends_on or []
            after_deps = action_resource.after or []
            if not deps and not after_deps:
                independent_actions += 1

        # Use threading if we have multiple independent actions
        return independent_actions >= 2

    def _initialize_actions(self, action: BaseAction) -> None:

        # Determine status of this run and update counters
        if self.initialize:

            if action.can_initialize():

                log.debug("(Re)Initializing action {} for run", action.action_name)
                try:
                    initialized = action.initialize()
                except Exception as e:
                    action.set_failed(f"Initialization error: {e}")
                    log.error("Failed to initialize action {}: {}", action.action_name, e)
                    initialized = False

                if not initialized:
                    log.debug("Action {} cannot be (re)initialized, keeping existing status", action.action_name)

            status = action.get_status()
            if status == StatusCode.PENDING:
                self.number_pending += 1
            elif status == StatusCode.RUNNING:
                self.number_running += 1
            elif status == StatusCode.COMPLETE:
                self.number_complete += 1
            elif status == StatusCode.FAILED:
                self.number_failed += 1

            log.debug("Action {} loaded with status: {}", action.action_name, action.get_status())

    def is_action_complete(self, action_resource: ActionResource) -> bool:
        """Check if action is complete."""
        with self.action_lock:
            action = self.actions.get(action_resource.action_key)
            if action is None:
                return False
            status = action.get_status()
            return status == StatusCode.COMPLETE

    def is_action_failed(self, action_resource: ActionResource) -> bool:
        """Check if action has failed."""
        with self.action_lock:
            action = self.actions.get(action_resource.action_key)
            if action is None:
                return False
            status = action.get_status()
            return status == StatusCode.FAILED

    def is_action_running(self, action_resource: ActionResource) -> bool:
        """Check if action is currently running."""
        with self.action_lock:
            action = self.actions.get(action_resource.action_key)
            if action is None:
                return False
            status = action.get_status()
            return status == StatusCode.RUNNING

    def is_action_pending(self, action_resource: ActionResource) -> bool:
        """Check if action is pending execution."""
        with self.action_lock:
            action = self.actions.get(action_resource.action_key)
            if action is None:
                return False
            status = action.get_status()
            return status == StatusCode.PENDING

    def run_action_execute(self, action_resource: ActionResource) -> None:
        """Execute actions in parallel using thread pool.

        Returns:
            Number of actions submitted for execution
        """
        if not self._dependents_complete(action_resource):
            log.debug("Action {} blocked by incomplete dependencies", action_resource.action_name)
            return

        action = self.actions.get(action_resource.action_key)

        if not action.can_execute():
            log.debug("Action {} cannot be executed in its current state: {}", action_resource.action_name, action.get_status())
            return

        if not self.slots.acquire(blocking=False):
            # pool “full” – defer submission
            log.debug("Execution slots full, deferring execution of action {}", action_resource.action_name)
            return

        if self.use_threading:
            with self.action_lock:
                future = self.executor.submit(self._execute_action_wrapper, action)
                future.add_done_callback(lambda f: self.slots.release())
                self.running_futures[action.action_name] = future
                log.debug("Submitted action {} to thread pool (worker thread)", action.action_name)
        else:
            self._execute_action_wrapper(action)
            self.slots.release()
            log.debug("Executed action {} in main thread", action.action_name)

    def _dependents_complete(self, action_resource: ActionResource) -> bool:
        """Check if all dependent actions are complete."""
        if len(action_resource.depends_on or []) == 0 and len(action_resource.after or []) == 0:
            return True

        # dependent names must be in the form of <namespace>/<action_name>
        for resource in action_resource.depends_on or []:
            action = self.actions.get(resource)
            if action is None or action.get_status() != StatusCode.COMPLETE:
                log.debug("Action {} blocked by dependency {}", action_resource.action_name, resource)
                return False

        for resource in action_resource.after or []:
            action = self.actions.get(resource)
            if action is None or action.get_status() != StatusCode.COMPLETE:
                log.debug("Action {} blocked by after dependency {}", action_resource.action_name, resource)
                return False

        return True

    def _execute_action_wrapper(self, action: BaseAction) -> None:
        """Thread-safe wrapper for action execution.

        This runs in a worker thread.
        """
        try:

            # Set thread-local logging context
            log.set_correlation_id(self.task_payload.correlation_id)
            log.set_identity(self.task_payload.deployment_details.get_identity())
            log.debug("Executing action {}", action.action_name)

            if action.is_pending():
                with self.action_lock:
                    self.number_pending -= 1
                    self.number_running += 1
                # If the action is pending, start its execution
                # the action itself will handle setting status to running
                action.execute()
            elif action.is_running():
                # If the action is already running, just check its status
                # The action itself will handle setting status to complete or failed
                action.check()

            with self.action_lock:
                if action.is_failed():
                    self.number_failed += 1
                elif action.is_complete():
                    self.number_complete += 1

        except Exception as e:
            action.set_failed(f"Execution error: {e}")
            log.error("Action {} failed in worker thread: {}", action.action_name, e)
        finally:
            with self.action_lock:
                self.number_running -= 1
                if action.action_name in self.running_futures:
                    del self.running_futures[action.action_name]
            log.reset_identity()

    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the thread pool executor."""
        if self.executor is not None:

            log.info("Shutting down thread pool (wait={})", wait)

            with self.action_lock:
                self.executor.shutdown(wait=wait)
                self.executor = None
                for future in self.running_futures.values():
                    future.cancel()
                self.running_futures.clear()

    def get_execution_summary(self) -> dict:
        """Get summary of execution state."""

        return {
            "total_actions": len(self.actions),
            "use_threading": self.use_threading,
            "pending": self.number_pending,
            "running": self.number_running,
            "complete": self.number_complete,
            "failed": self.number_failed,
        }
