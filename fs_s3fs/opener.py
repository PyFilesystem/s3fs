# coding: utf-8
"""Defines the S3FSpener."""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

__all__ = ['S3FSOpener']

from os import getenv

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
        strict = (
            parse_result.params['strict'] == '1'
            if 'strict' in parse_result.params
            else True
        )
        s3fs = S3FS(
            bucket_name,
            dir_path=dir_path or '/',
            aws_access_key_id=parse_result.username or getenv('aws_access_key_id', None),
            aws_secret_access_key=parse_result.password or getenv('aws_secret_access_key', None),
            endpoint_url=parse_result.params.get('endpoint_url', None),
            acl=parse_result.params.get('acl', None),
            cache_control=parse_result.params.get('cache_control', None),
            strict=strict
        )
        return s3fs
