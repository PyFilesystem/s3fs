from __future__ import unicode_literals

import unittest

from nose.plugins.attrib import attr

from fs.test import FSTestCases
from fs_s3fs import S3FS

import boto3


class TestS3FS(FSTestCases, unittest.TestCase):
    """Test S3FS implementation from dir_path."""

    bucket_name = "fsexample"
    s3 = boto3.resource("s3")
    client = boto3.client("s3")

    def make_fs(self):
        self._delete_bucket_contents()
        return S3FS(self.bucket_name)

    def _delete_bucket_contents(self):
        response = self.client.list_objects(Bucket=self.bucket_name)
        contents = response.get("Contents", ())
        for obj in contents:
            self.client.delete_object(Bucket=self.bucket_name, Key=obj["Key"])


@attr("slow")
class TestS3FSSubDir(FSTestCases, unittest.TestCase):
    """Test S3FS implementation from dir_path."""

    bucket_name = "fsexample"
    s3 = boto3.resource("s3")
    client = boto3.client("s3")

    def make_fs(self):
        self._delete_bucket_contents()
        self.s3.Object(self.bucket_name, "subdirectory").put()
        return S3FS(self.bucket_name, dir_path="subdirectory")

    def _delete_bucket_contents(self):
        response = self.client.list_objects(Bucket=self.bucket_name)
        contents = response.get("Contents", ())
        for obj in contents:
            self.client.delete_object(Bucket=self.bucket_name, Key=obj["Key"])


class TestS3FSHelpers(unittest.TestCase):
    def test_path_to_key(self):
        s3 = S3FS("foo")
        self.assertEqual(s3._path_to_key("foo.bar"), "foo.bar")
        self.assertEqual(s3._path_to_key("foo/bar"), "foo/bar")

    def test_path_to_key_subdir(self):
        s3 = S3FS("foo", "/dir")
        self.assertEqual(s3._path_to_key("foo.bar"), "dir/foo.bar")
        self.assertEqual(s3._path_to_key("foo/bar"), "dir/foo/bar")

    def test_upload_args(self):
        s3 = S3FS("foo", acl="acl", cache_control="cc")
        self.assertDictEqual(
            s3._get_upload_args("test.jpg"),
            {"ACL": "acl", "CacheControl": "cc", "ContentType": "image/jpeg"},
        )
        self.assertDictEqual(
            s3._get_upload_args("test.mp3"),
            {"ACL": "acl", "CacheControl": "cc", "ContentType": "audio/mpeg"},
        )
        self.assertDictEqual(
            s3._get_upload_args("test.json"),
            {"ACL": "acl", "CacheControl": "cc", "ContentType": "application/json"},
        )
        self.assertDictEqual(
            s3._get_upload_args("unknown.unknown"),
            {"ACL": "acl", "CacheControl": "cc", "ContentType": "binary/octet-stream"},
        )
