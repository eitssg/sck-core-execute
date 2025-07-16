"""Helper class for managing actions in the actionlib module."""

import re

from ..actionlib.action import BaseAction
from .factory import ActionFactory

from core_framework.models import TaskPayload, ActionSpec

from .actions.aws.kms.create_grants import CreateGrantsAction
from .actions.aws.rds.modify_db_instance import ModifyDbInstanceAction
from .actions.aws.copy_image import CopyImageAction
from .actions.aws.create_cloud_front_invalidation import (
    CreateCloudFrontInvalidationAction,
)
from .actions.aws.create_image import CreateImageAction
from .actions.aws.create_stack import CreateStackAction
from .actions.aws.delete_ecr_repository import DeleteEcrRepositoryAction
from .actions.aws.delete_image import DeleteImageAction
from .actions.aws.delete_security_group_enis import DeleteSecurityGroupEnisAction
from .actions.aws.delete_stack import DeleteStackAction
from .actions.aws.delete_user import DeleteUserAction
from .actions.aws.duplicate_image_to_account import DuplicateImageToAccountAction
from .actions.aws.empty_bucket import EmptyBucketAction
from .actions.aws.get_stack_outputs import GetStackOutputsAction
from .actions.aws.get_stack_references import GetStackReferencesAction
from .actions.aws.put_event import PutEventAction
from .actions.aws.put_metric_data import PutMetricDataAction
from .actions.aws.share_image import ShareImageAction
from .actions.aws.unprotect_elb import UnprotectELBAction
from .actions.aws.upload_context import UploadContextAction
from .actions.system.no_op import NoOpAction
from .actions.system.set_variables import SetVariablesAction


# There is an argument that suggests that each and every one of these should be a lambda function

valid_actions = [
    ("AWS::KMS::CreateGrants", CreateGrantsAction),
    ("AWS::RDS::ModifyDbInstance", ModifyDbInstanceAction),
    ("AWS::CopyImage", CopyImageAction),
    ("AWS::CreateCloudFrontInvalidation", CreateCloudFrontInvalidationAction),
    ("AWS::CreateImage", CreateImageAction),
    ("AWS::CreateStack", CreateStackAction),
    ("AWS::CreateStackSet", NoOpAction),
    ("AWS::DeleteEcrRepository", DeleteEcrRepositoryAction),
    ("AWS::DeleteImage", DeleteImageAction),
    ("AWS::DeleteSecurityGroupEnis", DeleteSecurityGroupEnisAction),
    ("AWS::DeleteStack", DeleteStackAction),
    ("AWS::DeleteStackSet", NoOpAction),
    ("AWS::DeleteUser", DeleteUserAction),
    ("AWS::DuplicateImageToAccount", DuplicateImageToAccountAction),
    ("AWS::EmptyBucket", EmptyBucketAction),
    ("AWS::GetStackOutputs", GetStackOutputsAction),
    ("AWS::GetStackReferences", GetStackReferencesAction),
    ("AWS::PutEvent", PutEventAction),
    ("AWS::PutMetricData", PutMetricDataAction),
    ("AWS::ShareImage", ShareImageAction),
    ("AWS::UnprotectElb", UnprotectELBAction),
    ("AWS::UploadContext", UploadContextAction),
    ("SYSTEM::NoOp", NoOpAction),
    ("SYSTEM::SetVariables", SetVariablesAction),
]


def is_valid_action(action_type: str) -> bool:
    """
    Check if the action name is valid.
    :param action_label: The action name to check.
    :return: True if the action name is valid, False otherwise.
    """
    return any(action_type == prefix for prefix, _ in valid_actions)


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
                lambda definition: ActionFactory.load(
                    definition, state_context, task_payload.deployment_details
                ),
                definitions,
            )
        )

    def pending_actions(self) -> list[BaseAction]:
        pending_actions = list(filter(lambda action: action.is_init(), self.actions))
        return pending_actions

    def completed_actions(self) -> list[BaseAction]:
        completed_actions = list(
            filter(lambda action: action.is_complete(), self.actions)
        )
        return completed_actions

    def incomplete_actions(self) -> list[BaseAction]:
        incompleted_actions = list(
            filter(lambda action: not action.is_complete(), self.actions)
        )
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
                if any(
                    self.__label_match(incomplete_action.name, dependency)
                    for dependency in pending_action.after
                ):
                    runnable = False
                    break

                # Check if any incomplete actions are blocking this action ("Before" mechanics on the incomplete action)
                # Can C run if:
                # - action A before action C
                # - action B before action C
                if any(
                    self.__label_match(pending_action.name, dependent)
                    for dependent in incomplete_action.before
                ):
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
