from __future__ import unicode_literals

import unittest

from fs.test import FSTestCases

from fs_s3fs import S3FS

import boto3


class TestS3FSSubDir(FSTestCases, unittest.TestCase):
    """Test S3FS implementation from dir_path."""
    bucket_name = 'fsexample'
    s3 = boto3.resource('s3')
    client = boto3.client('s3')

    def make_fs(self):
        self._delete_bucket_contents()
        self.s3.Object(self.bucket_name, 'subdirectory').put()
        return S3FS(self.bucket_name, dir_path='subdirectory')

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
