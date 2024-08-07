from shimoku_tangram.secrets import secrets_manager
from unittest import TestCase
from unittest.mock import patch


class TestSecretsManager(TestCase):
    @patch("shimoku_tangram.secrets.secrets_manager.boto3.client")
    def test_put_secret(self, mock_client):
        return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}
        mock_client.return_value.create_secret.return_value = return_value
        response = secrets_manager.put_secret("test", "test")

        self.assertEqual(response, True)

        mock_client.return_value.create_secret.assert_called_once_with(
            Name="test", SecretString="test"
        )

    @patch("shimoku_tangram.secrets.secrets_manager.boto3.client")
    def test_put_secret_dict(self, mock_client):
        return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}
        mock_client.return_value.create_secret.return_value = return_value
        response = secrets_manager.put_secret("test", {"test": "test"})

        self.assertEqual(response, True)

        mock_client.return_value.create_secret.assert_called_once_with(
            Name="test", SecretString='{"test": "test"}'
        )

    @patch("shimoku_tangram.secrets.secrets_manager.boto3.client")
    def test_get_secret(self, mock_client):
        return_value = {"SecretString": "test"}
        mock_client.return_value.get_secret_value.return_value = return_value
        response = secrets_manager.get_secret("test")

        self.assertEqual(response, "test")

        mock_client.return_value.get_secret_value.assert_called_once_with(
            SecretId="test"
        )

    @patch("shimoku_tangram.secrets.secrets_manager.boto3.client")
    def test_get_secret_dict(self, mock_client):
        return_value = {"SecretString": '{"test": "test"}'}
        mock_client.return_value.get_secret_value.return_value = return_value
        response = secrets_manager.get_secret("test", is_dict=True)

        self.assertEqual(response, {"test": "test"})

        mock_client.return_value.get_secret_value.assert_called_once_with(
            SecretId="test"
        )

    @patch("shimoku_tangram.secrets.secrets_manager.boto3.client")
    def test_delete_secret(self, mock_client):
        return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}
        mock_client.return_value.delete_secret.return_value = return_value
        response = secrets_manager.delete_secret("test")

        self.assertEqual(response, True)

        mock_client.return_value.delete_secret.assert_called_once_with(
            SecretId="test", ForceDeleteWithoutRecovery=True
        )
