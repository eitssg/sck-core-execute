"""Helper class for managing actions in the actionlib module."""

from typing import Any
import enum
import re

from ..actionlib.action import BaseAction
from .factory import ActionFactory

from core_framework.models import TaskPayload, ActionSpec


class FlowControl(enum.Enum):
    """Enum for flow control actions."""

    EXECUTE = "execute"
    FAILURE = "failure"
    SUCCESS = "success"

    @classmethod
    def from_value(cls, value: str | None) -> "FlowControl":
        """Convert a string value to a FlowControl enum."""
        if value is None:
            return cls.EXECUTE
        if value.lower() == "execute":
            return cls.EXECUTE
        elif value.lower() == "failure":
            return cls.FAILURE
        elif value.lower() == "success":
            return cls.SUCCESS
        else:
            raise ValueError(f"Invalid flow control value: {value}")

    def __str__(self):
        return self.value

    def __repr__(self):
        return f"FlowControl.{self.value.upper()}"


class Helper:
    """Generate BaseAction list from action definitions"""

    actions: list[BaseAction]

    def __init__(
        self,
        definitions: list[ActionSpec],
        state_context: dict,
        task_payload: TaskPayload,
    ):

        self.actions = list(
            map(
                lambda definition: ActionFactory.load(definition, state_context, task_payload.deployment_details),
                definitions,
            )
        )

    def pending_actions(self) -> list[BaseAction]:
        pending_actions = list(filter(lambda action: action.is_init(), self.actions))
        return pending_actions

    def completed_actions(self) -> list[BaseAction]:
        completed_actions = list(filter(lambda action: action.is_complete(), self.actions))
        return completed_actions

    def incomplete_actions(self) -> list[BaseAction]:
        incompleted_actions = list(filter(lambda action: not action.is_complete(), self.actions))
        return incompleted_actions

    def runnable_actions(self) -> list[BaseAction]:

        pending_actions = self.pending_actions()
        incomplete_actions = self.incomplete_actions()

        runnable_actions = []
        for pending_action in pending_actions:
            runnable = True
            # Check if any dependencies are incomplete
            for incomplete_action in incomplete_actions:

                # Actions can't block themselves
                if pending_action.name == incomplete_action.name:
                    continue

                # Check if any incomplete actions are blocking this action ("After" mechanics on the pending action)
                # Can C run if:
                # - action C after action A
                # - action C after action B
                if any(self.__label_match(incomplete_action.name, dependency) for dependency in pending_action.after):
                    runnable = False
                    break

                # Check if any incomplete actions are blocking this action ("Before" mechanics on the incomplete action)
                # Can C run if:
                # - action A before action C
                # - action B before action C
                if any(self.__label_match(pending_action.name, dependent) for dependent in incomplete_action.before):
                    runnable = False
                    break

            if runnable:
                runnable_actions.append(pending_action)

        return runnable_actions

    def running_actions(self) -> list[BaseAction]:
        running_actions = list(filter(lambda action: action.is_running(), self.actions))
        return running_actions

    def failed_actions(self) -> list[BaseAction]:
        failed_actions = list(filter(lambda action: action.is_failed(), self.actions))
        return failed_actions

    def __label_match(self, name: str, matcher: str) -> bool:
        # Split by the first '/' - need to treat wildcards differently
        splits = matcher.split("/", 1)

        # Process the base PRN (before the '/')
        base_prn = splits[0]
        base_prn = base_prn.replace("*", "[^:]*")

        # Process the PRN resource path (after the '/')
        if len(splits) == 2:
            path = splits[1]
            path = path.replace("*", ".*")

        # Produce the final regex (join base PRN with resource path)
        if len(splits) == 2:
            regex = "/".join([base_prn, path])
        else:
            regex = base_prn

        # Must match the entire string, not just the beginning of it
        regex = "^{}$".format(regex)

        return re.match(regex, name) is not None
