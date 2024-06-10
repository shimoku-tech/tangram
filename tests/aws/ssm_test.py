import unittest
from uuid import uuid4
from shimoku_tangram.aws import ssm
from os import getenv
from .mocks.MockSsmClient import get_mock_ssm_client

if getenv('MOCKING') == 'True':
    ssm.get_ssm_client = get_mock_ssm_client

class TestSSM(unittest.TestCase):

    def test_secret(self):
        secret_name = f'test-secret-{uuid4()}'
        secret_value = 'secret_value'
        
        ssm.put_secret(secret_name, secret_value)

        self.assertEqual(secret_value, ssm.get_secret(secret_name))
        ssm.delete_secret(secret_name)

        with self.assertRaises(ssm.get_ssm_client().exceptions.ParameterNotFound):
            ssm.get_secret(secret_name)