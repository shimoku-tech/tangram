from shimoku_tangram.orchestration import step_functions
from unittest import TestCase
from unittest.mock import patch

class TestStepFunctions(TestCase):
    @patch("shimoku_tangram.orchestration.step_functions.client")
    def test_send_task_success(self, mock_client):
        return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}
        mock_client.return_value.send_task_success.return_value = return_value
        response = step_functions.send_task_success("test", {"test": "test"})

        mock_client.return_value.send_task_success.assert_called_once_with(
            taskToken="test", output='{"test": "test"}'
        )

    @patch("shimoku_tangram.orchestration.step_functions.client")
    def test_send_task_failure(self, mock_client):
        return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}
        mock_client.return_value.send_task_failure.return_value = return_value
        response = step_functions.send_task_failure("test", "test")

        mock_client.return_value.send_task_failure.assert_called_once_with(
            taskToken="test", error="TaskFailed", cause="test"
        )