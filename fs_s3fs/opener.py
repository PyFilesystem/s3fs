# coding: utf-8
"""Defines the S3FSpener."""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

__all__ = ['S3FSOpener']

from fs.opener import Opener
from fs.opener.errors import OpenerError

from ._s3fs import S3FS


class S3FSOpener(Opener):
    protocols = ['s3']

    def open_fs(self, fs_url, parse_result, writeable, create, cwd):
        bucket_name, _, dir_path = parse_result.resource.partition('/')
        if not bucket_name:
            raise OpenerError(
                "invalid bucket name in '{}'".format(fs_url)
            )
        s3fs = S3FS(
            bucket_name,
            dir_path=dir_path or '/',
            aws_access_key_id=parse_result.username or None,
            aws_secret_access_key=parse_result.password or None,
        )
        return s3fs
