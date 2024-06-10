import unittest
from uuid import uuid4
from gzip import decompress
from shimoku_tangram.aws import s3
from shimoku_tangram.logging import init_logger
from .mocks.MockS3Client import get_mock_s3_client
from os import getenv

logger = init_logger(__name__)

test_counter = 0

if getenv('MOCKING') == 'True':
    s3.get_s3_client = get_mock_s3_client

class TestS3(unittest.TestCase):

    def setUp(self) -> None:
        global test_counter
        test_counter += 1
        self.bucket = f'test-shimoku-tangram'
        if not s3.bucket_exists(self.bucket):
            s3.create_bucket(self.bucket)
    
    def tearDown(self) -> None:
        global test_counter
        test_counter -= 1
        if not s3.bucket_exists(self.bucket):
            return
        if test_counter == 0:
            s3.clear_path(self.bucket)
        self.assertEqual(len(s3.list_objects_metadata(self.bucket)), 0)
        s3.delete_bucket(self.bucket)
        self.assertFalse(s3.bucket_exists(self.bucket))
 
    def test_zipped_object(self):
        logger.info('Testing zipped object')
        key = f'test_zipped_object-{uuid4()}'
        body = b'Hello World!'
        previous_objects_metadata = s3.list_objects_metadata(self.bucket)
        s3.put_object(self.bucket, key, body)
        self.assertEqual(body, s3.get_object(self.bucket, key))
        self.assertEqual(body, decompress(s3.get_object(self.bucket, key, compressed=False)))
        s3.delete_object(self.bucket, key)
        current_objects_metadata = s3.list_objects_metadata(self.bucket)
        self.assertEqual(len(previous_objects_metadata), len(current_objects_metadata))
        logger.info('Test zipped object passed')

    def test_text_object(self):
        logger.info('Testing text object')
        key = f'test_text_object-{uuid4()}'
        body = 'Hello World!'
        previous_objects_metadata = s3.list_objects_metadata(self.bucket)
        s3.put_text_object(self.bucket, key, body, compress=False)
        self.assertEqual(body, s3.get_text_object(self.bucket, key, compressed=False))
        s3.delete_object(self.bucket, key)
        current_objects_metadata = s3.list_objects_metadata(self.bucket)
        self.assertEqual(len(previous_objects_metadata), len(current_objects_metadata))
        logger.info('Test text object passed')

    def test_json_object(self):
        logger.info('Testing json object')
        key = f'test_json_object-{uuid4()}'
        body = {'hello': 'world'}
        previous_objects_metadata = s3.list_objects_metadata(self.bucket)
        s3.put_json_object(self.bucket, key, body, compress=False)
        self.assertEqual(body, s3.get_json_object(self.bucket, key, compressed=False))
        s3.delete_object(self.bucket, key)
        current_objects_metadata = s3.list_objects_metadata(self.bucket) 
        self.assertEqual(len(previous_objects_metadata), len(current_objects_metadata))
        logger.info('Test json object passed')
