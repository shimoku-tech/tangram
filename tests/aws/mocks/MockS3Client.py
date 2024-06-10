from io import BytesIO

class MockS3ClientExceptions:
    class ClientError(Exception):
        pass

class MockS3Client:

    exceptions = MockS3ClientExceptions()
    buckets = {}

    def head_bucket(self, Bucket):
        if Bucket not in self.buckets:
            raise self.exceptions.ClientError()
        
    def create_bucket(self, Bucket, **kwargs):
        self.buckets[Bucket] = {}
        return {'ResponseMetadata': {'HTTPStatusCode': 200}}

    def delete_bucket(self, Bucket, **kwargs):
        del self.buckets[Bucket]
        return {'ResponseMetadata': {'HTTPStatusCode': 200}}

    def put_object(self, Bucket, Key, Body):
        self.buckets[Bucket][Key] = Body
        return {'ResponseMetadata': {'HTTPStatusCode': 200}}

    def get_object(self, Bucket, Key):
        return {'Body': BytesIO(self.buckets[Bucket][Key])}

    def delete_object(self, Bucket, Key):
        del self.buckets[Bucket][Key]
        return {'ResponseMetadata': {'HTTPStatusCode': 200}}

    def list_objects_v2(self, Bucket, Prefix):
        return {
            'Contents': [
                {'Key': key, 'Body': BytesIO(self.buckets[Bucket][key])}
                for key in self.buckets[Bucket]
                if key.startswith(Prefix)
            ]
        }

client = None

def get_mock_s3_client():
    global client
    if client is None:
        client = MockS3Client()
    return client
