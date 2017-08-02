from __future__ import unicode_literals

import unittest

from fs.test import FSTestCases

from s3fs import S3FS

import boto3

class TestS3FS(FSTestCases, unittest.TestCase):
    """Test OSFS implementation."""
    bucket_name = 'fsexample'
    s3 = boto3.resource('s3')
    client = boto3.client('s3')

    def make_fs(self):
        self._delete_bucket_contents()
        return S3FS(self.bucket_name)

    def _delete_bucket_contents(self):
        response = self.client.list_objects(
            Bucket=self.bucket_name
        )
        contents = response.get("Contents", ())
        for obj in contents:
            self.client.delete_object(
                Bucket=self.bucket_name,
                Key=obj["Key"]
            )

    def destroy_fs(self, fs):
        pass
