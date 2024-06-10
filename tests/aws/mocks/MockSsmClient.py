
class MockSsmExceptions:
    class ParameterNotFound(Exception):
        pass

class MockSsmClient:
    secrets = {}
    exceptions = MockSsmExceptions()
    
    def put_parameter(self, Name, Value, Type, Overwrite):
        self.secrets[Name] = Value
        return {'ResponseMetadata': {'HTTPStatusCode': 200}}
    
    def get_parameter(self, Name, WithDecryption):
        if Name not in self.secrets:
            raise self.exceptions.ParameterNotFound()
        return {'Parameter': {'Value': self.secrets[Name]}}
    
    def delete_parameter(self, Name):
        if Name not in self.secrets:
            raise self.exceptions.ParameterNotFound()
        del self.secrets[Name]
        return {'ResponseMetadata': {'HTTPStatusCode': 200}}

client = None

def get_mock_ssm_client():
    global client
    if client is None:
        client = MockSsmClient()
    return client
