from shimoku_tangram.storage import s3
from gzip import compress
from io import BytesIO
from unittest import TestCase
from unittest.mock import patch


class TestS3(TestCase):
    @patch("shimoku_tangram.storage.s3.client")
    def test_bucket_exists(self, mock_client):
        mock_client.return_value.head_bucket.return_value = {}
        response = s3.bucket_exists("test")

        self.assertTrue(response)

        mock_client.return_value.head_bucket.assert_called_once_with(Bucket="test")

    @patch("shimoku_tangram.storage.s3.client")
    def test_bucket_does_not_exist(self, mock_client):
        mock_client.return_value.head_bucket.side_effect = RuntimeError
        mock_client.return_value.exceptions.ClientError = RuntimeError
        response = s3.bucket_exists("test")

        self.assertFalse(response)

        mock_client.return_value.head_bucket.assert_called_once_with(Bucket="test")

    @patch("shimoku_tangram.storage.s3.client")
    def test_list_objects_metadata(self, mock_client):
        mock_client.return_value.list_objects_v2.return_value = {
            "Contents": [{"Key": "test"}]
        }
        response = s3.list_objects_metadata("test")

        self.assertEqual(response, [{"Key": "test"}])

        mock_client.return_value.list_objects_v2.assert_called_once_with(
            Bucket="test", Prefix=""
        )

    @patch("shimoku_tangram.storage.s3.client")
    def test_list_objects_key(self, mock_client):
        mock_client.return_value.list_objects_v2.return_value = {
            "Contents": [{"Key": "test"}]
        }
        response = s3.list_objects_key("test")

        self.assertEqual(response, ["test"])

        mock_client.return_value.list_objects_v2.assert_called_once_with(
            Bucket="test", Prefix=""
        )

    @patch("shimoku_tangram.storage.s3.client")
    def test_list_objects_metadata_no_contents(self, mock_client):
        mock_client.return_value.list_objects_v2.return_value = {}
        response = s3.list_objects_metadata("test")

        self.assertEqual(response, [])

        mock_client.return_value.list_objects_v2.assert_called_once_with(
            Bucket="test", Prefix=""
        )

    @patch("shimoku_tangram.storage.s3.client")
    def test_clear_path(self, mock_client):
        mock_client.return_value.list_objects_v2.return_value = {
            "Contents": [{"Key": "test"}]
        }
        mock_client.return_value.delete_object.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200}
        }
        response = s3.clear_path("test")

        self.assertTrue(response)

        mock_client.return_value.list_objects_v2.assert_called_once_with(
            Bucket="test", Prefix=""
        )
        mock_client.return_value.delete_object.assert_called_once_with(
            Bucket="test", Key="test"
        )

    @patch("shimoku_tangram.storage.s3.client")
    def test_clear_path_empty(self, mock_client):
        mock_client.return_value.list_objects_v2.return_value = {}
        response = s3.clear_path("test")

        self.assertTrue(response)

        mock_client.return_value.list_objects_v2.assert_called_once_with(
            Bucket="test", Prefix=""
        )

    @patch("shimoku_tangram.storage.s3.client")
    def test_clear_path_error(self, mock_client):
        mock_client.return_value.list_objects_v2.return_value = {
            "Contents": [{"Key": "test"}]
        }
        mock_client.return_value.delete_object.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 400}
        }
        response = s3.clear_path("test")

        self.assertFalse(response)

        mock_client.return_value.list_objects_v2.assert_called_once_with(
            Bucket="test", Prefix=""
        )
        mock_client.return_value.delete_object.assert_called_once_with(
            Bucket="test", Key="test"
        )

    @patch("shimoku_tangram.storage.s3.client")
    def test_get_object_compressed(self, mock_client):
        class MockBody:
            def read(self):
                return compress(b"test")

        mock_client.return_value.get_object.return_value = {"Body": MockBody()}
        response = s3.get_object("test", "test")

        self.assertEqual(response, b"test")

        mock_client.return_value.get_object.assert_called_once_with(
            Bucket="test", Key="test"
        )

    @patch("shimoku_tangram.storage.s3.client")
    def test_get_object_uncompressed(self, mock_client):
        class MockBody:
            def read(self):
                return b"test"

        mock_client.return_value.get_object.return_value = {"Body": MockBody()}

        response = s3.get_object("test", "test", compressed=False)

        self.assertEqual(response, b"test")

        mock_client.return_value.get_object.assert_called_once_with(
            Bucket="test", Key="test"
        )

    @patch("shimoku_tangram.storage.s3.client")
    def test_get_object_error(self, mock_client):
        mock_client.return_value.get_object.side_effect = RuntimeError

        with self.assertRaises(RuntimeError):
            s3.get_object("test", "test")

        mock_client.return_value.get_object.assert_called_once_with(
            Bucket="test", Key="test"
        )

    @patch("shimoku_tangram.storage.s3.client")
    def test_put_object_compressed(self, mock_client):
        mock_client.return_value.put_object.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200}
        }
        response = s3.put_object("test", "test", b"test")

        self.assertTrue(response)

        mock_client.return_value.put_object.assert_called_once_with(
            Bucket="test", Key="test", Body=compress(b"test")
        )

    @patch("shimoku_tangram.storage.s3.client")
    def test_put_object_uncompressed(self, mock_client):
        mock_client.return_value.put_object.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200}
        }
        response = s3.put_object("test", "test", b"test", compress=False)

        self.assertTrue(response)

        mock_client.return_value.put_object.assert_called_once_with(
            Bucket="test", Key="test", Body=b"test"
        )

    @patch("shimoku_tangram.storage.s3.client")
    def test_put_object_error(self, mock_client):
        mock_client.return_value.put_object.side_effect = RuntimeError

        with self.assertRaises(RuntimeError):
            s3.put_object("test", "test", b"test")

        mock_client.return_value.put_object.assert_called_once_with(
            Bucket="test", Key="test", Body=compress(b"test")
        )

    @patch("shimoku_tangram.storage.s3.client")
    def test_delete_object(self, mock_client):
        mock_client.return_value.delete_object.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200}
        }
        response = s3.delete_object("test", "test")

        self.assertTrue(response)

        mock_client.return_value.delete_object.assert_called_once_with(
            Bucket="test", Key="test"
        )

    @patch("shimoku_tangram.storage.s3.client")
    def test_delete_non_existent_object(self, mock_client):
        mock_client.return_value.delete_object.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 400}
        }
        response = s3.delete_object("test", "test")

        self.assertFalse(response)

        mock_client.return_value.delete_object.assert_called_once_with(
            Bucket="test", Key="test"
        )

    @patch("shimoku_tangram.storage.s3.client")
    def test_get_text_object(self, mock_client):
        mock_client.return_value.get_object.return_value = {
            "Body": BytesIO(compress(b"test"))
        }
        response = s3.get_text_object("test", "test")

        self.assertEqual(response, "test")

        mock_client.return_value.get_object.assert_called_once_with(
            Bucket="test", Key="test"
        )

    @patch("shimoku_tangram.storage.s3.client")
    def test_get_text_object_non_utf8(self, mock_client):
        mock_client.return_value.get_object.return_value = {
            "Body": BytesIO(compress("test".encode("cp1252")))
        }
        response = s3.get_text_object("test", "test", encoding="cp1252")

        self.assertEqual(response, "test")

        mock_client.return_value.get_object.assert_called_once_with(
            Bucket="test", Key="test"
        )

    @patch("shimoku_tangram.storage.s3.client")
    def test_put_text_object(self, mock_client):
        mock_client.return_value.put_object.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200}
        }
        response = s3.put_text_object("test", "test", "test")

        self.assertTrue(response)

        mock_client.return_value.put_object.assert_called_once_with(
            Bucket="test", Key="test", Body=compress(b"test")
        )

    @patch("shimoku_tangram.storage.s3.client")
    def test_put_text_object_non_utf8(self, mock_client):
        mock_client.return_value.put_object.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200}
        }
        response = s3.put_text_object("test", "test", "test", encoding="cp1252")

        self.assertTrue(response)

        mock_client.return_value.put_object.assert_called_once_with(
            Bucket="test", Key="test", Body=compress("test".encode("cp1252"))
        )

    @patch("shimoku_tangram.storage.s3.client")
    def test_get_json_object(self, mock_client):
        mock_client.return_value.get_object.return_value = {
            "Body": BytesIO(compress(b'{"test": "test"}'))
        }
        response = s3.get_json_object("test", "test")

        self.assertEqual(response, {"test": "test"})

        mock_client.return_value.get_object.assert_called_once_with(
            Bucket="test", Key="test"
        )

    @patch("shimoku_tangram.storage.s3.client")
    def test_put_json_object(self, mock_client):
        mock_client.return_value.put_object.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200}
        }
        response = s3.put_json_object("test", "test", {"test": "test"})

        self.assertTrue(response)

        mock_client.return_value.put_object.assert_called_once_with(
            Bucket="test", Key="test", Body=compress(b'{"test": "test"}')
        )
