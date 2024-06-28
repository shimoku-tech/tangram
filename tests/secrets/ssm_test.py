from shimoku_tangram.secrets import ssm
from unittest import TestCase
from unittest.mock import patch


class TestSSM(TestCase):
    @patch("shimoku_tangram.secrets.ssm.client")
    def test_put_parameter(self, mock_client):
        return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}
        mock_client.return_value.put_parameter.return_value = return_value
        response = ssm.put_parameter("test", "test")

        self.assertEqual(response, return_value)

        mock_client.return_value.put_parameter.assert_called_once_with(
            Name="test", Value="test", Type="SecureString", Overwrite=True
        )

    @patch("shimoku_tangram.secrets.ssm.client")
    def test_get_parameter(self, mock_client):
        return_value = {"Parameter": {"Value": "test"}}
        mock_client.return_value.get_parameter.return_value = return_value
        response = ssm.get_parameter("test")

        self.assertEqual(response, "test")

        mock_client.return_value.get_parameter.assert_called_once_with(
            Name="test", WithDecryption=True
        )

    @patch("shimoku_tangram.secrets.ssm.client")
    def test_delete_parameter(self, mock_client):
        return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}
        mock_client.return_value.delete_parameter.return_value = return_value
        response = ssm.delete_parameter("test")

        self.assertEqual(response, return_value)

        mock_client.return_value.delete_parameter.assert_called_once_with(Name="test")
