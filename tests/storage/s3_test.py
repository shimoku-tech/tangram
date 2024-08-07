import pandas as pd
from shimoku_tangram.storage import s3
from collections import namedtuple
from gzip import compress
from io import BytesIO
import json
import pickle
import re
from uuid import UUID
from unittest import TestCase
from unittest.mock import patch


# Object to be used in the tests
AuxTestObject = namedtuple("AuxTestObject", ["attribute"])


class TestS3(TestCase):
    @staticmethod
    def is_valid_key(string: str) -> bool:
        try:
            return bool(
                re.match(
                    r"^path/([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})\..*\.gz$",
                    string,
                )
                and UUID(string[5:41])
            )
        except ValueError:
            return False

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
    def test_list_objects_metadata_no_contents(self, mock_client):
        mock_client.return_value.list_objects_v2.return_value = {}
        response = s3.list_objects_metadata("test")

        self.assertEqual(response, [])

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
    def test_list_single_object_key_single_file(self, mock_client):
        mock_client.return_value.list_objects_v2.return_value = {
            "Contents": [{"Key": "file1"}]
        }

        response = s3.list_single_object_key("", "")

        self.assertEqual(response, "file1")

        mock_client.return_value.list_objects_v2.assert_called_once_with(
            Bucket="", Prefix=""
        )

    @patch("shimoku_tangram.storage.s3.client")
    def test_list_single_object_key_empty(self, mock_client):
        mock_client.return_value.list_objects_v2.return_value = {}

        with self.assertRaises(ValueError) as context:
            s3.list_single_object_key("", "")

        self.assertEqual(str(context.exception), "File not found.")

        mock_client.return_value.list_objects_v2.assert_called_once_with(
            Bucket="", Prefix=""
        )

    @patch("shimoku_tangram.storage.s3.client")
    def test_list_single_object_key_multiple_files(self, mock_client):
        mock_client.return_value.list_objects_v2.return_value = {
            "Contents": [{"Key": "file1"}, {"Key": "file2"}]
        }

        with self.assertRaises(ValueError) as context:
            s3.list_single_object_key("", "")

        self.assertEqual(str(context.exception), "Multiple files found.")

        mock_client.return_value.list_objects_v2.assert_called_once_with(
            Bucket="", Prefix=""
        )

    @patch("shimoku_tangram.storage.s3.client")
    def test_list_multiple_objects_keys_multiple_files(self, mock_client):
        mock_client.return_value.list_objects_v2.return_value = {
            "Contents": [{"Key": "file1"}, {"Key": "file2"}]
        }

        response = s3.list_multiple_objects_keys("", "")

        self.assertEqual(response, ["file1", "file2"])

        mock_client.return_value.list_objects_v2.assert_called_once_with(
            Bucket="", Prefix=""
        )

    @patch("shimoku_tangram.storage.s3.client")
    def test_list_multiple_objects_keys_empty(self, mock_client):
        mock_client.return_value.list_objects_v2.return_value = {}

        with self.assertRaises(ValueError) as context:
            s3.list_multiple_objects_keys("", "")

        self.assertEqual(str(context.exception), "No files found.")

        mock_client.return_value.list_objects_v2.assert_called_once_with(
            Bucket="", Prefix=""
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

    @patch("shimoku_tangram.storage.s3.client")
    def test_get_pkl_object(self, mock_client):
        expected = AuxTestObject(attribute="value")

        mock_client.return_value.get_object.return_value = {
            "Body": BytesIO(compress(pickle.dumps(expected)))
        }

        response = s3.get_pkl_object("", "")

        self.assertEqual(response, expected)

        mock_client.return_value.get_object.assert_called_once_with(Bucket="", Key="")

    @patch("shimoku_tangram.storage.s3.client")
    def test_put_pkl_object(self, mock_client):
        expected = AuxTestObject(attribute="value")

        mock_client.return_value.put_object.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200}
        }

        response = s3.put_pkl_object("", "", expected)

        self.assertTrue(response)

    def test_get_extension_compressed(self):
        expected = "pkl"

        key = "path/file.pkl.gz"
        actual = s3.get_extension(key, compressed=True)

        self.assertEqual(actual, expected)

    def test_get_extension_uncompressed(self):
        expected = "json"

        key = "path/file.json"
        actual = s3.get_extension(key, compressed=False)

        self.assertEqual(actual, expected)

    def test_is_compressed_true(self):
        expected = True

        key = "path/file.json.gz"
        actual = s3.is_compressed(key)

        self.assertEqual(actual, expected)

    def test_is_compressed_false(self):
        expected = False

        key = "path/file.csv"
        actual = s3.is_compressed(key)

        self.assertEqual(actual, expected)

    @patch("shimoku_tangram.storage.s3.client")
    def test_get_single_json_object_single_json_uncompressed(self, mock_client):
        expected = {"key": "value"}

        mock_client.return_value.list_objects_v2.return_value = {
            "Contents": [{"Key": "path/file.json"}]
        }
        mock_client.return_value.get_object.return_value = {
            "Body": BytesIO(json.dumps(expected).encode("utf-8"))
        }
        actual = s3.get_single_json_object("", "")

        self.assertEqual(actual, expected)

    @patch("shimoku_tangram.storage.s3.client")
    def test_get_single_json_object_single_json_compressed(self, mock_client):
        expected = {"key": "value"}

        mock_client.return_value.list_objects_v2.return_value = {
            "Contents": [{"Key": "path/file.json.gz"}]
        }
        mock_client.return_value.get_object.return_value = {
            "Body": BytesIO(compress(json.dumps(expected).encode("utf-8")))
        }
        actual = s3.get_single_json_object("", "")

        self.assertEqual(actual, expected)

    @patch("shimoku_tangram.storage.s3.client")
    def test_get_single_json_object_multiples_json(self, mock_client):
        mock_client.return_value.list_objects_v2.return_value = {
            "Contents": [{"Key": "path/file.json"}, {"Key": "path/file2.json"}]
        }

        with self.assertRaises(ValueError) as context:
            s3.get_single_json_object("", "")

        self.assertEqual(str(context.exception), "Multiple files found.")

    @patch("shimoku_tangram.storage.s3.client")
    def test_get_single_json_object_single_nojson(self, mock_client):
        mock_client.return_value.list_objects_v2.return_value = {
            "Contents": [{"Key": "path/file.csv"}]
        }

        with self.assertRaises(ValueError) as context:
            s3.get_single_json_object("", "")

        self.assertEqual(str(context.exception), "File is not a json file.")

    @patch("shimoku_tangram.storage.s3.client")
    def test_get_single_pkl_object_single_pkl_uncompressed(self, mock_client):
        expected = AuxTestObject(attribute="value")

        mock_client.return_value.list_objects_v2.return_value = {
            "Contents": [{"Key": "path/file.pkl"}]
        }
        mock_client.return_value.get_object.return_value = {
            "Body": BytesIO(pickle.dumps(expected))
        }
        actual = s3.get_single_pkl_object("", "")

        self.assertEqual(actual, expected)

    @patch("shimoku_tangram.storage.s3.client")
    def test_get_single_pkl_object_single_pkl_compressed(self, mock_client):
        expected = AuxTestObject(attribute="value")

        mock_client.return_value.list_objects_v2.return_value = {
            "Contents": [{"Key": "path/file.pkl.gz"}]
        }
        mock_client.return_value.get_object.return_value = {
            "Body": BytesIO(compress(pickle.dumps(expected)))
        }
        actual = s3.get_single_pkl_object("", "")

        self.assertEqual(actual, expected)

    @patch("shimoku_tangram.storage.s3.client")
    def test_get_single_pkl_object_multiples_pkl(self, mock_client):
        mock_client.return_value.list_objects_v2.return_value = {
            "Contents": [{"Key": "path/file.pkl"}, {"Key": "path/file2.pkl"}]
        }

        with self.assertRaises(ValueError) as context:
            s3.get_single_pkl_object("", "")

        self.assertEqual(str(context.exception), "Multiple files found.")

    @patch("shimoku_tangram.storage.s3.client")
    def test_get_single_pkl_object_single_nopkl(self, mock_client):
        mock_client.return_value.list_objects_v2.return_value = {
            "Contents": [{"Key": "path/file.csv"}]
        }

        with self.assertRaises(ValueError) as context:
            s3.get_single_pkl_object("", "")

        self.assertEqual(str(context.exception), "File is not a pickle file.")

    @patch("shimoku_tangram.storage.s3.client")
    def test_get_multiple_csv_objects_single_csv_uncompressed(self, mock_client):
        expected = pd.DataFrame({"1": [1, 2, 3, 4, 5]})

        mock_client.return_value.list_objects_v2.return_value = {
            "Contents": [{"Key": "path/file.csv"}]
        }
        mock_client.return_value.get_object.return_value = {
            "Body": BytesIO(expected.to_csv(index=False).encode("utf-8"))
        }
        actual = s3.get_multiple_csv_objects("", "")

        self.assertTrue(actual.equals(expected))

    @patch("shimoku_tangram.storage.s3.client")
    def test_get_multiple_csv_objects_single_csv_compressed(self, mock_client):
        expected = pd.DataFrame({"1": [1, 2, 3, 4, 5]})

        mock_client.return_value.list_objects_v2.return_value = {
            "Contents": [{"Key": "path/file.csv.gz"}]
        }
        mock_client.return_value.get_object.return_value = {
            "Body": BytesIO(compress(expected.to_csv(index=False).encode("utf-8")))
        }
        actual = s3.get_multiple_csv_objects("", "")

        self.assertTrue(actual.equals(expected))

    @patch("shimoku_tangram.storage.s3.client")
    def test_get_multiple_csv_objects_multiple_csv(self, mock_client):
        df1 = pd.DataFrame({"1": [1, 2, 3, 4, 5]})
        df2 = pd.DataFrame({"1": [6, 7, 8, 9, 10]})
        expected = pd.concat([df1, df2]).reset_index(drop=True)

        mock_client.return_value.list_objects_v2.return_value = {
            "Contents": [{"Key": "path/file1.csv"}, {"Key": "path/file2.csv"}]
        }
        mock_client.return_value.get_object.side_effect = [
            {"Body": BytesIO(df1.to_csv(index=False).encode("utf-8"))},
            {"Body": BytesIO(df2.to_csv(index=False).encode("utf-8"))},
        ]
        actual = s3.get_multiple_csv_objects("", "")

        self.assertTrue(actual.equals(expected))

    @patch("shimoku_tangram.storage.s3.client")
    def test_get_multiple_csv_objects_nocsv(self, mock_client):
        mock_client.return_value.list_objects_v2.return_value = {
            "Contents": [{"Key": "path/file1.csv"}, {"Key": "path/file2.json"}]
        }

        with self.assertRaises(ValueError) as context:
            s3.get_multiple_csv_objects("", "")

        self.assertEqual(str(context.exception), "Not all files are csv files.")

    @patch("shimoku_tangram.storage.s3.client")
    def test_put_single_json_object(self, mock_client):
        expected = {"key": "value"}

        mock_client.return_value.put_object.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200}
        }

        response = s3.put_single_json_object("", "path", expected)

        self.assertTrue(self.is_valid_key(response))

    @patch("shimoku_tangram.storage.s3.client")
    def test_put_single_pkl_object(self, mock_client):
        expected = AuxTestObject(attribute="value")
        mock_client.return_value.put_object.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200}
        }
        response = s3.put_single_pkl_object("", "path", expected)

        self.assertTrue(self.is_valid_key(response))

    @patch("shimoku_tangram.storage.s3.client")
    def test_put_multiple_csv_objects(self, mock_client):
        expected = pd.DataFrame({"1": [1, 2, 3, 4, 5]})

        mock_client.return_value.put_object.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200}
        }
        response = s3.put_multiple_csv_objects("", "path", expected)

        self.assertTrue(all([self.is_valid_key(res) for res in response]))

    @patch("shimoku_tangram.storage.s3.client")
    def test_put_multiple_csv_objects_empty_df(self, mock_client):
        expected = pd.DataFrame()
        s3.put_multiple_csv_objects("", "path", expected)
        mock_client.return_value.put_object.assert_not_called()
