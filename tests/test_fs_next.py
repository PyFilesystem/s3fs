"""Run FS Next's contract on an isolated Moto backend; no AWS account is used."""

import unittest
from importlib.metadata import PackageNotFoundError, requires, version

import boto3
from moto import mock_aws
from packaging.requirements import Requirement
import fs
from fs import open_fs
from fs.test import FSTestCases
from fs_s3fs import S3FS


class TestFSNext(FSTestCases, unittest.TestCase):
    prefix = ""

    def setUp(self):
        self.mock = mock_aws()
        self.mock.start()
        self.addCleanup(self.mock.stop)
        client = boto3.client(
            "s3",
            region_name="us-east-1",
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
        )
        client.create_bucket(Bucket="fs-next-tests")
        super().setUp()

    def make_fs(self):
        return S3FS(
            "fs-next-tests",
            dir_path=self.prefix,
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            region="us-east-1",
        )

    def test_distribution_and_opener(self):
        self.assertTrue(version("fs-next"))
        self.assertIn("site-packages", fs.__file__)
        with self.assertRaises(PackageNotFoundError):
            version("fs")
        url = "s3://testing:testing@fs-next-tests"
        with open_fs(url) as storage:
            storage.writetext("한글.txt", "FS Next")
        with open_fs(url) as storage:
            self.assertEqual(storage.readtext("한글.txt"), "FS Next")
            storage.remove("한글.txt")


class TestFSNextPrefix(TestFSNext):
    prefix = "subdirectory"


def test_dependency_markers():
    dependencies = [Requirement(value) for value in requires("fs-s3fs")]
    for python_version, expected in [
        ("3.9", "fs"),
        ("3.10", "fs-next"),
        ("3.15", "fs-next"),
    ]:
        active = [
            dependency.name
            for dependency in dependencies
            if dependency.name in ("fs", "fs-next")
            and dependency.marker.evaluate({"python_version": python_version})
        ]
        assert active == [expected]
