from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import os
import tempfile
import threading

import boto3
from botocore.exceptions import ClientError

import six
from six import text_type

from fs import ResourceType
from fs.base import FS
from fs.info import Info
from fs import errors
from fs.mode import Mode
from fs.path import abspath, basename, normpath, relpath
from fs.time import datetime_to_epoch


def _make_repr(class_name, *args, **kwargs):
    """
    Generate a repr string.

    Positional arguments should be the positional arguments used to
    construct the class. Keyword arguments should consist of tuples of
    the attribute value and default. If the value is the default, then
    it won't be rendered in the output.

    Here's an example::

        def __repr__(self):
            return make_repr('MyClass', 'foo', name=(self.name, None))

    The output of this would be something line ``MyClass('foo',
    name='Will')``.

    """
    arguments = [repr(arg) for arg in args]
    arguments.extend([
        "{}={!r}".format(name, value)
        for name, (value, default) in sorted(kwargs.items())
        if value != default
    ])
    return "{}({})".format(class_name, ', '.join(arguments))



class S3File(object):

    @classmethod
    def factory(cls, filename, mode, on_close):
        f  = tempfile.TemporaryFile()
        proxy = cls(f, filename, mode, on_close=on_close)
        return proxy

    def __repr__(self):
        return _make_repr(
            self.__class__.__name__,
            self.__filename,
            text_type(self.__mode)
        )

    def __init__(self, f, filename, mode, on_close=None):
        self._f = f
        self.__filename = filename
        self.__mode = mode
        self._on_close = on_close

    def close(self):
        print("Close")
        if self._on_close is not None:
            self._on_close(self._f)

    def __getattr__(self, key):
        return getattr(self._f, key)


@six.python_2_unicode_compatible
class S3FS(FS):

    _meta = {
        'case_insensitive': False,
        'invalid_path_chars': '\0',
        'network': True,
        'read_only': False,
        'thread_safe': True,
        'unicode_paths': True,
        'virtual': False,
    }

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
        super(S3FS, self).__init__()

    def __repr__(self):
        return _make_repr(
            self.__class__.__name__,
            self._bucket_name,
            region=(self.region, None),
            delimiter=(self.delimiter, '/')
        )

    def __str__(self):
        fmt = "<{} '{}'>"
        return fmt.format(
            self.__class__.__name__.lower(),
            self._bucket_name
        )

    def _path_to_key(self, path):
        """Converts an fs path to a s3 key."""
        return relpath(
            normpath(path)
        ).replace('/', self.delimiter)

    def _path_to_dir_key(self, path):
        """Converts an fs path to a s3 key."""
        _key = relpath(
            normpath(path)
        ).replace('/', self.delimiter)
        return _key + self.delimiter if _key else _key

    def _key_to_path(self, key):
        return key.replace(self.delimiter, '/')

    def _get_object(self, path, key):
        try:
            obj = self.s3.Object(self._bucket_name, key + self.delimiter)
            obj.load()
        except ClientError as error:
            error_code = int(error.response['Error']['Code'])
            if error_code == 404:
                try:
                    obj = self.s3.Object(self._bucket_name, key.rstrip(self.delimiter))
                    obj.load()
                except ClientError as error:
                    error_code = int(error.response['Error']['Code'])
                    if error_code == 404:
                        raise errors.ResourceNotFound(path)
                else:
                    return obj
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
        _key = self._path_to_key(_path)

        obj = self._get_object(path, _key)

        name = basename(self._key_to_path(_key))
        is_dir = obj.key.endswith(self.delimiter)

        info = {
            'basic': {
                'name': name,
                'is_dir': is_dir
            }
        }

        _type = int(ResourceType.directory if is_dir else ResourceType.file)
        if 'details' in namespaces:
            info['details'] = {
                'accessed': None,
                'modified': datetime_to_epoch(obj.last_modified),
                'size': obj.content_length,
                'type': _type
            }

        return Info(info)

    def listdir(self, path):
        _path = self.validatepath(path)
        _s3_key = self._path_to_dir_key(_path)
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
            for obj in result.get('Contents', ()):
                name = obj["Key"][prefix_len:]
                if name:
                    _directory.append(name)

        if not _directory:
            if not self.getinfo(_path).is_dir:
                raise errors.DirectoryExpected(path)

        return _directory

    def makedir(self, path, permission=None, recreate=False):
        self.check()
        _path = self.validatepath(path)
        _key = self._path_to_dir_key(_path)

        try:
            self.getinfo(path)
        except errors.ResourceNotFound:
            pass
        else:
            if recreate:
                return self.opendir(_path)
            else:
                raise errors.DirectoryExists(path)
        self.s3.Object(self._bucket_name, _key).put()
        return self.opendir(_path)

    def openbin(self, path, mode="r", buffering=-1, **options):
        _mode = Mode(mode)
        _mode.validate_bin()
        self.check()
        _path = self.validatepath(path)
        _key = self._path_to_key(_path)

        if _mode.create:

            def on_close(proxy_file):
                proxy_file.seek(0)
                self.client.upload_fileobj(proxy_file, self._bucket_name, _key)
                return True

            if _mode.exclusive:
                try:
                    self.getinfo(path)
                except errors.ResourceNotFound:
                    pass
                else:
                    raise errors.FileExists(path)

            proxy_file = S3File.factory(path, _mode, on_close=on_close)
            if _mode.appending:
                self.client.download_fileobj(self._bucket_name, _key, proxy_file)
                proxy_file.seek(0, os.SEEK_END)

            return proxy_file

        info = self.getinfo(path)
        if info.is_dir:
            raise errors.FileExpected(path)

        def on_close(proxy_file):
            if _mode.writing:
                proxy_file.seek(0, os.SEEK_SET)
                self.client.upload_fileobj(proxy_file, self._bucket_name, _key)
            return True

        proxy_file = S3File.factory(path, _mode, on_close=on_close)
        self.client.download_fileobj(self._bucket_name, _key, proxy_file)
        proxy_file.seek(0, os.SEEK_SET)

        return proxy_file


    def remove(self, path):
        self.check()
        _path = self.validatepath(path)
        _key = self._path_to_key(_path)
        info = self.getinfo(_path)
        if info.is_dir:
            raise errors.FileExpected(path)
        self.client.delete_bucket(
            Bucket=self._bucket_name,
            Key=_key
        )

    def isempty(self, path):
        self.check()
        _path = self.validatepath()
        _key = self._path_to_key(_path)
        response = self.client.list_objects(
            Bucket=self._bucket_name,
            Prefix=_key + self.delimiter,
            MaxKeys=2,
        )
        contents = response.get("Contents", ())
        for obj in contents:
            if obj["Key"] != _key:
                return False
        return True

    def removedir(self, path):
        self.check()
        _path = self.validatepath(path)
        _key = self._path_to_key(_path)
        if _path == '/':
            raise errors.RemoveRootError()
        info = self.getinfo(_path)
        if not info.is_dir:
            raise errors.DirectoryExpected(path)
        if not self.isempty(path):
            raise errors.DirectoryNotEmpty(path)
        self.client.delete_bucket(
            Bucket=self._bucket_name,
            Key=_key
        )

    def setinfo(self, path, info):
        self.getinfo(path)
