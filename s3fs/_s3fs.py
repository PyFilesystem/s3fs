from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import threading

import boto3
from botocore.exceptions import ClientError

from fs import ResourceType
from fs.base import FS
from fs import errors
from fs.path import abspath, basename, normpath, relpath
from fs.time import datetime_to_epoch


class S3FS(FS):

    def __init__(self,
                 bucket_name,
                 aws_access_key_id=None,
                 aws_secret_access_key=None,
                 region=None,
                 delimiter='/'):
        self._bucket_name = bucket_name
        self.region = region
        self.delimiter = delimiter

        self._bucket = None
        self._client = None

        self._tlocal = threading.local()

    def __repr__(self):
        _fmt = "{}({!r}, region={!r}, delimiter={!r})"
        return _fmt.format(
            self.__class__.__name__,
            self._bucket_name,
            self.region,
            self.delimiter
        )

    def __str__(self):
        fmt = "<{} '{}'>"
        return fmt.format(
            self.__class__.__name__.lower(),
            self._bucket_name
        )

    def path_to_key(self, path):
        """Converts an fs path to a s3 key."""
        return relpath(
            normpath(path)
        ).replace('/', self.delimiter)

    def path_to_dir_key(self, path):
        """Converts an fs path to a s3 key."""
        _key = relpath(
            normpath(path)
        ).replace('/', self.delimiter)
        return _key + self.delimiter if _key else _key

    def key_to_path(self, key):
        return key.replace(self.delimiter, '/')

    def _get_object(self, path, key):
        try:
            obj = self.s3.Object(self._bucket_name, key)
        except ClientError as error:
            error_code = int(error.response['Error']['Code'])
            if error_code == 404:
                raise errors.ResourceNotFound(path)
            raise errors.ResourceError(path)
        else:
            return obj

    @property
    def s3(self):
        if not hasattr(self._tlocal, 's3'):
            self._tlocal.s3 = boto3.resource('s3')
        return self._tlocal.s3

    @property
    def client(self):
        if not hasattr(self._tlocal, 'client'):
            self._tlocal.client = boto3.client('s3')
        return self._tlocal.client

    @property
    def bucket(self):
        """Get a the filesystem bucket."""
        if self._bucket is None:
            self._bucket = self.s3.Bucket(self._bucket_name)
        return self._bucket

    def getinfo(self, path, namespaces=None):
        self.check()
        namespaces = namespaces or ()
        _path = self.validatepath(path)
        _key = self.path_to_key(_path)

        obj = self._get_object(path, _key)

        name = basename(self.key_to_path(_key))
        _children = self.bucket.objects.filter(
            Prefix=_key + self.delimiter,
            Delimiter=self.delimiter,
        ).limit(1)
        is_dir = bool(len(_children))

        info = {
            'basic': {
                'name': name,
                'is_dir': is_dir
            }
        }

        _type = (ResourceType.directory if is_dir else ResourceType.file)
        if 'details' in namespaces:
            info['details'] = {
                'access': None,
                'modified': datetime_to_epoch(obj.last_modified),
                'size': obj.content_length,
                'type': _type
            }

        return info

    def listdir(self, path):
        _path = self.validatepath(path)
        _s3_key = self.path_to_dir_key(_path)
        prefix_len = len(_s3_key)

        paginator = self.client.get_paginator('list_objects')
        _paginate = paginator.paginate(
            Bucket=self._bucket_name,
            Prefix=_s3_key,
            Delimiter=self.delimiter
        )
        _directory = []
        for result in _paginate:
            common_prefixes = result.get('CommonPrefixes', ())
            for prefix in common_prefixes:
                _prefix = prefix.get('Prefix')
                _name = _prefix[prefix_len:]
                if _name != _s3_key:
                    _directory.append(_name.rstrip(self.delimiter))
            for obj in result['Contents']:
                name = obj["Key"][prefix_len:]
                if name:
                    _directory.append(name)

        return _directory

        objects = self.bucket.objects.filter(
            Prefix=_s3_key,
            Delimiter=self.delimiter
        )
        prefix_len = len(_s3_key)
        _directory = [
            obj.key[prefix_len:]
            for obj in objects
            if obj.key != _s3_key
        ]
        return _directory

    def makedir(self, path, permission=None, recreate=False):
        self.check()
        _path = self.validatepath(path)
        _key = self.path_to_key(_path)

        try:
            info = self.getinfo(path)
        except errors.ResourceNotFound:
            pass
        else:
            if recreate:
                return
            else:
                raise errors.DirectoryExists(path)
        _key = _key + self.delimiter
        response = self.s3.Object(self._bucket_name, _key).put()

    def openbin(self, path, mode="r", buffering=-1, **options):
        pass

    def remove(self, path):
        pass

    def removedir(self, path):
        pass

    def setinfo(self, path, info):
        pass
