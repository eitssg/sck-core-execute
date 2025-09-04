import pytest
import io
import json
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone
from typing import Any
from core_framework.models import TaskPayload, ActionResource, FileDetails
from core_execute.actionlib.helper import Helper, FlowControl, ActionStatus
from core_execute.actionlib.action import BaseAction

from core_execute.execute import (
    timeout_imminent,
    run_state_machine,
    execute_lifecycle_hooks,
    load_actions,
    load_state,
    save_state,
    save_actions,
    _pluralize,
    _percentage,
    __bootup_time__,
    __max_runtime__,
)


class TestTimeoutImminent:
    """Test the timeout_imminent function."""

    def test_timeout_imminent_with_lambda_context_safe(self):
        """Test timeout_imminent with Lambda context - safe time remaining."""
        context = Mock()
        context.get_remaining_time_in_millis.return_value = 30000  # 30 seconds

        result = timeout_imminent(context)

        assert result is False
        context.get_remaining_time_in_millis.assert_called_once()

    def test_timeout_imminent_with_lambda_context_imminent(self):
        """Test timeout_imminent with Lambda context - timeout imminent."""
        context = Mock()
        context.get_remaining_time_in_millis.return_value = 5000  # 5 seconds

        result = timeout_imminent(context)

        assert result is True
        context.get_remaining_time_in_millis.assert_called_once()

    def test_timeout_imminent_without_context_safe(self):
        """Test timeout_imminent without context - safe time remaining."""
        with patch("core_execute.execute.datetime") as mock_datetime:
            # Mock current time to be 1 minute after bootup
            mock_datetime.now.return_value.timestamp.return_value = __bootup_time__ + 60

            result = timeout_imminent(None)

            assert result is False

    def test_timeout_imminent_without_context_imminent(self):
        """Test timeout_imminent without context - timeout imminent."""
        with patch("core_execute.execute.datetime") as mock_datetime:
            # Mock current time to be 9 minutes 55 seconds after bootup (5 seconds remaining)
            mock_datetime.now.return_value.timestamp.return_value = __bootup_time__ + (9 * 60 + 55)

            result = timeout_imminent(None)

            assert result is True

    def test_timeout_imminent_without_context_exceeded(self):
        """Test timeout_imminent without context - time exceeded."""
        with patch("core_execute.execute.datetime") as mock_datetime:
            # Mock current time to be 11 minutes after bootup (exceeded max runtime)
            mock_datetime.now.return_value.timestamp.return_value = __bootup_time__ + (11 * 60)

            result = timeout_imminent(None)

            assert result is True


class TestRunStateMachine:
    """Test the run_state_machine function."""

    @pytest.fixture
    def mock_helper(self):
        """Create a mock Helper instance."""
        helper = Mock(spec=Helper)
        helper.get_execution_summary.return_value = {"total": 3, "complete": 1, "running": 1, "failed": 0}
        helper.has_critical_failures.return_value = False
        helper.get_running_actions.return_value = []
        helper.get_runnable_actions.return_value = []
        helper.execution_complete.return_value = False
        helper.execution_successful.return_value = False
        helper.executed_this_run = set()
        return helper

    @pytest.fixture
    def mock_action(self):
        """Create a mock BaseAction instance."""
        action = Mock(spec=BaseAction)
        action.name = "test_action"
        action.definition = Mock()
        action.definition.lifecycle_hooks = None
        action.is_complete.return_value = False
        action.is_failed.return_value = False
        return action

    def test_run_state_machine_critical_failure_fast_fail(self, mock_helper):
        """Test run_state_machine with critical failures - should fail fast."""
        mock_helper.has_critical_failures.return_value = True

        result = run_state_machine(mock_helper, None)

        assert result == FlowControl.FAILURE
        mock_helper.get_execution_summary.assert_called_once()

    def test_run_state_machine_successful_completion(self, mock_helper):
        """Test run_state_machine with successful completion."""
        mock_helper.execution_complete.return_value = True
        mock_helper.execution_successful.return_value = True

        result = run_state_machine(mock_helper, None)

        assert result == FlowControl.SUCCESS

    def test_run_state_machine_failed_completion(self, mock_helper):
        """Test run_state_machine with failed completion."""
        mock_helper.execution_complete.return_value = True
        mock_helper.execution_successful.return_value = False
        mock_helper.get_failed_actions.return_value = []

        result = run_state_machine(mock_helper, None)

        assert result == FlowControl.FAILURE

    def test_run_state_machine_continue_execution(self, mock_helper):
        """Test run_state_machine continuing execution."""
        mock_helper.get_pending_actions.return_value = [Mock(name="pending_action")]
        mock_helper.get_running_actions.return_value = [Mock(name="running_action")]

        result = run_state_machine(mock_helper, None)

        assert result == FlowControl.EXECUTE

    def test_run_state_machine_stuck_execution(self, mock_helper):
        """Test run_state_machine with stuck execution (no runnable or running actions)."""
        pending_action = Mock()
        pending_action.name = "stuck_action"
        mock_helper.executed_this_run = {"stuck_action"}
        mock_helper.get_pending_actions.return_value = [pending_action]
        mock_helper.get_running_actions.return_value = []

        result = run_state_machine(mock_helper, None)

        assert result == FlowControl.FAILURE

    @patch("core_execute.execute.execute_lifecycle_hooks")
    def test_run_state_machine_running_action_completes(self, mock_lifecycle, mock_helper, mock_action):
        """Test run_state_machine with running action that completes."""
        mock_action.is_complete.return_value = True
        mock_helper.get_running_actions.return_value = [mock_action]
        mock_helper.execution_complete.return_value = True
        mock_helper.execution_successful.return_value = True

        result = run_state_machine(mock_helper, None)

        assert result == FlowControl.SUCCESS
        mock_action.check.assert_called_once()
        mock_helper.update_action_status.assert_called_with("test_action", ActionStatus.COMPLETE)
        mock_lifecycle.assert_called_once_with(mock_helper, mock_action, "post_complete")

    @patch("core_execute.execute.execute_lifecycle_hooks")
    def test_run_state_machine_running_action_fails(self, mock_lifecycle, mock_helper, mock_action):
        """Test run_state_machine with running action that fails."""
        mock_action.is_failed.return_value = True
        mock_helper.get_running_actions.return_value = [mock_action]
        mock_helper.execution_complete.return_value = True
        mock_helper.execution_successful.return_value = False
        mock_helper.get_failed_actions.return_value = [mock_action]

        result = run_state_machine(mock_helper, None)

        assert result == FlowControl.FAILURE
        mock_helper.update_action_status.assert_called_with("test_action", ActionStatus.FAILED)
        mock_lifecycle.assert_called_once_with(mock_helper, mock_action, "post_failure")

    @patch("core_execute.execute.execute_lifecycle_hooks")
    def test_run_state_machine_executes_runnable_action(self, mock_lifecycle, mock_helper, mock_action):
        """Test run_state_machine executing a runnable action."""
        mock_action.is_complete.return_value = True
        mock_helper.get_runnable_actions.return_value = [mock_action]
        mock_helper.execution_complete.return_value = True
        mock_helper.execution_successful.return_value = True

        result = run_state_machine(mock_helper, None)

        assert result == FlowControl.SUCCESS
        mock_action.execute.assert_called_once()
        mock_helper.update_action_status.assert_any_call("test_action", ActionStatus.RUNNING)
        mock_helper.update_action_status.assert_any_call("test_action", ActionStatus.COMPLETE)

    @patch("core_execute.execute.timeout_imminent")
    def test_run_state_machine_timeout_during_running_check(self, mock_timeout, mock_helper, mock_action):
        """Test run_state_machine with timeout during running action check."""
        mock_timeout.return_value = True
        mock_helper.get_running_actions.return_value = [mock_action, Mock(name="action2")]
        mock_helper.execution_complete.return_value = True
        mock_helper.execution_successful.return_value = True

        result = run_state_machine(mock_helper, None)

        # Should break after first action due to timeout
        assert mock_action.check.call_count <= 1

    @patch("core_execute.execute.timeout_imminent")
    def test_run_state_machine_timeout_during_execution(self, mock_timeout, mock_helper, mock_action):
        """Test run_state_machine with timeout during action execution."""
        mock_timeout.return_value = True
        mock_helper.get_runnable_actions.return_value = [mock_action, Mock(name="action2")]
        mock_helper.execution_complete.return_value = True
        mock_helper.execution_successful.return_value = True

        result = run_state_machine(mock_helper, None)

        # Should break after attempting first action due to timeout
        assert mock_action.execute.call_count <= 1


class TestExecuteLifecycleHooks:
    """Test the execute_lifecycle_hooks function."""

    @pytest.fixture
    def mock_helper(self):
        """Create a mock Helper instance."""
        helper = Mock(spec=Helper)
        helper.action_instances = {}
        helper.action_status = {}
        helper.executed_this_run = set()
        helper._create_action_instance.return_value = Mock(spec=BaseAction)
        return helper

    @pytest.fixture
    def mock_parent_action(self):
        """Create a mock parent action."""
        action = Mock(spec=BaseAction)
        action.name = "parent_action"
        action.definition = Mock()
        return action

    @pytest.fixture
    def mock_hook_resource(self):
        """Create a mock hook ActionResource."""
        hook = Mock(spec=ActionResource)
        hook.action_name = "hook_action"
        return hook

    def test_execute_lifecycle_hooks_no_hooks(self, mock_helper, mock_parent_action):
        """Test execute_lifecycle_hooks with no hooks defined."""
        mock_parent_action.definition.lifecycle_hooks = None

        execute_lifecycle_hooks(mock_helper, mock_parent_action)

        # Should return early, no helper methods called
        assert not mock_helper._create_action_instance.called

    def test_execute_lifecycle_hooks_empty_hooks(self, mock_helper, mock_parent_action):
        """Test execute_lifecycle_hooks with empty hooks list."""
        mock_parent_action.definition.lifecycle_hooks = []

        execute_lifecycle_hooks(mock_helper, mock_parent_action)

        assert not mock_helper._create_action_instance.called

    def test_execute_lifecycle_hooks_creates_new_hook(self, mock_helper, mock_parent_action, mock_hook_resource):
        """Test execute_lifecycle_hooks creating new hook action."""
        mock_parent_action.definition.lifecycle_hooks = [mock_hook_resource]
        hook_action = Mock(spec=BaseAction)
        hook_action.is_complete.return_value = True
        mock_helper._create_action_instance.return_value = hook_action

        execute_lifecycle_hooks(mock_helper, mock_parent_action)

        mock_helper._create_action_instance.assert_called_once_with(mock_hook_resource)
        assert "hook_action" in mock_helper.action_instances
        assert mock_helper.action_status["hook_action"] == ActionStatus.PENDING
        hook_action.execute.assert_called_once()

    def test_execute_lifecycle_hooks_uses_existing_hook(self, mock_helper, mock_parent_action, mock_hook_resource):
        """Test execute_lifecycle_hooks using existing hook action."""
        mock_parent_action.definition.lifecycle_hooks = [mock_hook_resource]
        existing_hook = Mock(spec=BaseAction)
        existing_hook.is_complete.return_value = True
        mock_helper.action_instances["hook_action"] = existing_hook
        mock_helper.action_status["hook_action"] = ActionStatus.PENDING

        execute_lifecycle_hooks(mock_helper, mock_parent_action)

        mock_helper._create_action_instance.assert_not_called()
        existing_hook.execute.assert_called_once()

    def test_execute_lifecycle_hooks_skips_executed_hook(self, mock_helper, mock_parent_action, mock_hook_resource):
        """Test execute_lifecycle_hooks skipping already executed hook."""
        mock_parent_action.definition.lifecycle_hooks = [mock_hook_resource]
        mock_helper.executed_this_run = {"hook_action"}
        existing_hook = Mock(spec=BaseAction)
        mock_helper.action_instances["hook_action"] = existing_hook
        mock_helper.action_status["hook_action"] = ActionStatus.PENDING

        execute_lifecycle_hooks(mock_helper, mock_parent_action)

        existing_hook.execute.assert_not_called()

    def test_execute_lifecycle_hooks_skips_non_pending_hook(self, mock_helper, mock_parent_action, mock_hook_resource):
        """Test execute_lifecycle_hooks skipping non-pending hook."""
        mock_parent_action.definition.lifecycle_hooks = [mock_hook_resource]
        existing_hook = Mock(spec=BaseAction)
        mock_helper.action_instances["hook_action"] = existing_hook
        mock_helper.action_status["hook_action"] = ActionStatus.COMPLETE

        execute_lifecycle_hooks(mock_helper, mock_parent_action)

        existing_hook.execute.assert_not_called()

    def test_execute_lifecycle_hooks_failed_hook(self, mock_helper, mock_parent_action, mock_hook_resource):
        """Test execute_lifecycle_hooks with hook that fails."""
        mock_parent_action.definition.lifecycle_hooks = [mock_hook_resource]
        hook_action = Mock(spec=BaseAction)
        hook_action.is_complete.return_value = False
        hook_action.is_failed.return_value = True
        mock_helper._create_action_instance.return_value = hook_action

        execute_lifecycle_hooks(mock_helper, mock_parent_action)

        mock_helper.update_action_status.assert_any_call("hook_action", ActionStatus.FAILED)


class TestS3Operations:
    """Test S3 operation functions."""

    @pytest.fixture
    def mock_task_payload(self):
        """Create a mock TaskPayload."""
        payload = Mock(spec=TaskPayload)
        payload.package = None

        # Mock actions details
        actions_details = Mock(spec=FileDetails)
        actions_details.bucket_name = "test-bucket"
        actions_details.bucket_region = "us-east-1"
        actions_details.key = "actions.yaml"
        actions_details.content_type = "application/x-yaml"
        payload.actions = actions_details

        # Mock state details
        state_details = Mock(spec=FileDetails)
        state_details.bucket_name = "test-bucket"
        state_details.bucket_region = "us-east-1"
        state_details.key = "state.yaml"
        state_details.content_type = "application/x-yaml"
        state_details.version_id = "v123"
        state_details.get_full_path.return_value = "test-bucket/state.yaml"
        payload.state = state_details

        return payload

    @patch("core_execute.execute.MagicS3Client")
    @patch("core_execute.execute.util")
    def test_load_actions_from_s3_yaml(self, mock_util, mock_s3_client, mock_task_payload):
        """Test load_actions from S3 with YAML content."""
        # Setup mocks
        mock_client = Mock()
        mock_s3_client.get_client.return_value = mock_client
        mock_client.download_fileobj.return_value = {"ContentType": "application/x-yaml", "VersionId": "v123"}

        mock_util.is_yaml_mimetype.return_value = True
        mock_util.read_yaml.return_value = [{"action_name": "test_action", "action_type": "test"}]

        result = load_actions(mock_task_payload)

        assert len(result) == 1
        assert isinstance(result[0], ActionResource)
        mock_client.download_fileobj.assert_called_once()

    @patch("core_execute.execute.MagicS3Client")
    @patch("core_execute.execute.util")
    def test_load_actions_from_embedded_package(self, mock_util, mock_s3_client, mock_task_payload):
        """Test load_actions from embedded package."""
        # Setup embedded package
        mock_package = Mock()
        mock_action = Mock(spec=ActionResource)
        mock_package.actions = [mock_action]
        mock_task_payload.package = mock_package

        result = load_actions(mock_task_payload)

        assert result == [mock_action]
        mock_s3_client.get_client.assert_not_called()

    def test_load_actions_no_actions(self, mock_task_payload):
        """Test load_actions with no actions in payload."""
        mock_task_payload.actions = None
        mock_task_payload.package = None

        with pytest.raises(ValueError, match="No actions found in the task payload"):
            load_actions(mock_task_payload)

    @patch("core_execute.execute.MagicS3Client")
    @patch("core_execute.execute.util")
    def test_load_actions_s3_error(self, mock_util, mock_s3_client, mock_task_payload):
        """Test load_actions with S3 error."""
        mock_client = Mock()
        mock_s3_client.get_client.return_value = mock_client
        mock_client.download_fileobj.side_effect = Exception("S3 error")

        with pytest.raises(Exception, match="Failed to load actions from S3"):
            load_actions(mock_task_payload)

    @patch("core_execute.execute.MagicS3Client")
    @patch("core_execute.execute.util")
    def test_load_state_success(self, mock_util, mock_s3_client, mock_task_payload):
        """Test load_state successful operation."""
        mock_client = Mock()
        mock_s3_client.get_client.return_value = mock_client
        mock_client.download_fileobj.return_value = {"ContentType": "application/x-yaml", "VersionId": "v123"}

        mock_util.is_yaml_mimetype.return_value = True
        mock_util.read_yaml.return_value = {"key": "value"}

        result = load_state(mock_task_payload)

        assert result == {"key": "value"}
        mock_client.download_fileobj.assert_called_once()

    def test_load_state_new_state(self, mock_task_payload):
        """Test load_state with new state creation."""
        mock_task_payload.state.version_id = "new"

        result = load_state(mock_task_payload)

        assert result == {}

    def test_load_state_no_state(self, mock_task_payload):
        """Test load_state with no state in payload."""
        mock_task_payload.state = None

        with pytest.raises(ValueError, match="No state found in the task payload"):
            load_state(mock_task_payload)

    @patch("core_execute.execute.MagicS3Client")
    @patch("core_execute.execute.util")
    def test_save_state_success(self, mock_util, mock_s3_client, mock_task_payload):
        """Test save_state successful operation."""
        mock_client = Mock()
        mock_s3_client.get_client.return_value = mock_client
        mock_client.put_object.return_value = Mock(version_id="v124")

        mock_util.is_yaml_mimetype.return_value = True
        mock_util.to_yaml.return_value = "serialized_data"

        state_data = {"key": "value"}
        save_state(mock_task_payload, state_data)

        mock_client.put_object.assert_called_once()
        assert mock_task_payload.state.version_id == "v124"

    @patch("core_execute.execute.MagicS3Client")
    @patch("core_execute.execute.util")
    def test_save_actions_success(self, mock_util, mock_s3_client, mock_task_payload):
        """Test save_actions successful operation."""
        mock_client = Mock()
        mock_s3_client.get_client.return_value = mock_client
        mock_client.put_object.return_value = Mock(version_id="v124")

        mock_util.to_yaml.return_value = "serialized_data"

        mock_action = Mock(spec=ActionResource)
        mock_action.model_dump.return_value = {"action_name": "test"}
        actions = [mock_action]

        save_actions(mock_task_payload, actions)

        mock_client.put_object.assert_called_once()
        mock_action.model_dump.assert_called_once()

    def test_save_actions_invalid_action_type(self, mock_task_payload):
        """Test save_actions with invalid action type."""
        actions = ["not_an_action_resource"]

        with pytest.raises(TypeError, match="Expected ActionResource"):
            save_actions(mock_task_payload, actions)


class TestHelperFunctions:
    """Test utility helper functions."""

    def test_pluralize_singular(self):
        """Test _pluralize with singular count."""
        result = _pluralize("action", 1)
        assert result == "1 action"

    def test_pluralize_plural(self):
        """Test _pluralize with plural count."""
        result = _pluralize("action", 3)
        assert result == "3 actions"

    def test_pluralize_zero(self):
        """Test _pluralize with zero count."""
        result = _pluralize("action", 0)
        assert result == "0 actions"

    def test_percentage_normal(self):
        """Test _percentage with normal values."""
        result = _percentage(3, 10)
        assert result == "30%"

    def test_percentage_zero_denominator(self):
        """Test _percentage with zero denominator."""
        result = _percentage(5, 0)
        assert result == "100%"

    def test_percentage_exact_division(self):
        """Test _percentage with exact division."""
        result = _percentage(5, 5)
        assert result == "100%"

    def test_percentage_rounding(self):
        """Test _percentage with rounding."""
        result = _percentage(1, 3)
        assert result == "33%"  # 33.333... rounded down to 33
