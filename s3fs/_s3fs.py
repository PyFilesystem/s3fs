from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import boto3

from fs.base import FS
from fs.path import abspath, basename, normpath, relpath


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

        self._s3 = boto3.resource('s3')
        self._bucket = None

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

    def key_to_path(self, key):
        return key.replace(self.delimiter, '/')

    @property
    def s3(self):
        return self._s3

    @property
    def bucket(self):
        """Get a the filesystem bucket."""
        if self._bucket is None:
            self._bucket = self._s3.Bucket(self._bucket_name)
        return self._bucket

    def getinfo(self, path, namespaces=None):
        self.check()
        namespaces = namespaces or ()
        _path = self.validatepath(path)
        _key = self.path_to_key(_path)

        obj = self.s3.Object(self._bucket_name, _key)

        name = basename(self.key_to_path(_key))
        _children = self.s3.objects.filter(
            prefix=_key + self.delimiter,
            Delimiter=self.delimiter,
        ).limit(1)
        is_dir = bool(list(_children))

        info = {
            'basic': {
                'name': name,
                'is_dir': is_dir
            }
        }
        return info

    def listdir(self, path):
        _path = self.validatepath(path)
        _s3_key = self.path_to_key(_path)
        objects = self.bucket.objects.filter(
            Prefix=_s3_key,
            Delimiter=self.delimiter
        )
        return [obj.key for obj in objects]

    def makedir(self, path, permission=None, recreate=False):
        pass

    def openbin(self, path, mode="r", buffering=-1, **options):
        pass

    def remove(self, path):
        pass

    def removedir(self, path):
        pass

    def setinfo(self, path, info):
        pass
