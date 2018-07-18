import json
from minio import Minio

class S3ResultStore(object):
    def __init__(self,resultstore):
        self.s3client = Minio(resultstore['host'],
            access_key = resultstore['access_key'],
            secret_key = resultstore['secret_key'],
        )
        self.s3bucket = resultstore['bucket']

    def get(self, resultid):
        return json.loads(self.s3client.get_object(self.s3bucket,resultid).read())
