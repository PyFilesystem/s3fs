from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

__all__ = ['S3FS']

from datetime import datetime
import io
import itertools
import os
from ssl import SSLError
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
from fs.path import basename, dirname, normpath, relpath
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



class S3File(io.IOBase):

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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    @property
    def raw(self):
        return self._f

    def close(self):
        if self._on_close is not None:
            self._on_close(self)

    @property
    def closed(self):
        return self._f.closed

    def fileno(self):
        return self._f.fileno()

    def flush(self):
        return self._f.flush()

    def isatty(self):
        return self._f.asatty()

    def readable(self):
        return self.__mode.reading

    def readline(self, limit=-1):
        return self._f.readline(limit)

    def readlines(self, hint=-1):
        if hint == -1:
            return self._f.readlines(hint)
        else:
            size = 0
            lines = []
            for line in iter(self._f.readline, b''):
                lines.append(line)
                size += len(line)
                if size > hint:
                    break
            return lines

    def seek(self, offset, whence=os.SEEK_SET):
        if whence not in (os.SEEK_CUR, os.SEEK_END, os.SEEK_SET):
            raise ValueError("invalid value for 'whence'")
        return self._f.seek(offset, whence)

    def seekable(self):
        return True

    def tell(self):
        return self._f.tell()

    def writable(self):
        return self.__mode.writing

    def writelines(self, lines):
        return self._f.writelines(lines)

    def read(self, n=-1):
        if not self.__mode.reading:
            raise IOError('not open for reading')
        return self._f.read(n)

    def readall(self):
        return self._f.readall()

    def readinto(self, b):
        return self._f.readinto()

    def write(self, b):
        if not self.__mode.writing:
            raise IOError('not open for reading')
        return self._f.write(b)

    def truncate(self, size=None):
        if size is None:
            size = self._f.tell()
        return self._f.truncate(size)


class S3ClientErrors(object):

    def __init__(self, path):
        self.path = path

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is ClientError:
            error = exc_value
            error_code = int(error.response['Error']['Code'])
            if error_code == 404:
                raise errors.ResourceNotFound(self.path)
        elif exc_type is SSLError:
            raise errors.OperationFailed(self.path, exc=exc_value)


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

    _object_attributes = [
        'accept_ranges',
        'cache_control',
        'content_disposition',
        'content_encoding',
        'content_language',
        'content_length',
        'content_type',
        'delete_marker',
        'e_tag',
        'expiration',
        'expires',
        'last_modified',
        'metadata',
        'missing_meta',
        'parts_count',
        'replication_status',
        'request_charged',
        'restore',
        'server_side_encryption',
        'sse_customer_algorithm',
        'sse_customer_key_md5',
        'ssekms_key_id',
        'storage_class',
        'version_id',
        'website_redirect_location'
    ]

    def __init__(self,
                 bucket_name,
                 aws_access_key_id=None,
                 aws_secret_access_key=None,
                 region=None,
                 delimiter='/'):
        self._bucket_name = bucket_name
        self.region = region
        self.delimiter = delimiter

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
        _key = key.rstrip(self.delimiter)
        try:
            obj = self.s3.Object(self._bucket_name, _key)
            obj.load()
        except ClientError as error:
            error_code = int(error.response['Error']['Code'])
            if error_code == 404:
                try:
                    obj = self.s3.Object(self._bucket_name, _key + self.delimiter)
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

    def _info_from_object(self, obj, namespaces):
        """Make an info dict from an s3 Object."""
        key = obj.key
        path = self._key_to_path(key)
        name = basename(path.rstrip('/'))
        is_dir = key.endswith(self.delimiter)
        info = {
            "basic": {
                "name": name,
                "is_dir": is_dir
            }
        }
        if 'details' in namespaces:
            _type = int(
                ResourceType.directory if is_dir
                else ResourceType.file
            )
            info['details'] = {
                'accessed': None,
                'modified': datetime_to_epoch(obj.last_modified),
                'size': obj.content_length,
                'type': _type
            }
        if 's3' in namespaces:
            s3info = info['s3'] = {}
            for name in self._object_attributes:
                value = getattr(obj, name, None)
                if isinstance(value, datetime):
                    value = datetime_to_epoch(value)
                s3info[name] = value

        return info

    def getinfo(self, path, namespaces=None):
        self.check()
        namespaces = namespaces or ()
        _path = self.validatepath(path)
        _key = self._path_to_key(_path)

        try:
            dir_path = dirname(_path)
            if dir_path != '/':
                _dir_key = self._path_to_dir_key(dir_path)
                self._get_object(dir_path, _dir_key)
        except errors.ResourceNotFound:
            raise errors.ResourceNotFound(path)

        if _path == '/':
            return Info({
                "basic":
                {
                    "name": "",
                    "is_dir": True
                },
                "details":
                {
                    "type": int(ResourceType.directory)
                }
            })

        obj = self._get_object(path, _key)
        info = self._info_from_object(obj, namespaces)
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

    def makedir(self, path, permissions=None, recreate=False):
        self.check()
        _path = self.validatepath(path)
        _key = self._path_to_dir_key(_path)

        if not self.isdir(dirname(_path)):
            raise errors.ResourceNotFound(path)

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
        return self.opendir(path)

    def openbin(self, path, mode="r", buffering=-1, **options):
        _mode = Mode(mode)
        _mode.validate_bin()
        self.check()
        _path = self.validatepath(path)
        _key = self._path_to_key(_path)

        if _mode.create:

            def on_close(s3file):
                try:
                    s3file.raw.seek(0)
                    self.client.upload_fileobj(s3file.raw, self._bucket_name, _key)
                finally:
                    s3file.raw.close()

            try:
                info = self.getinfo(path)
            except errors.ResourceNotFound:
                pass
            else:
                if _mode.exclusive:
                    raise errors.FileExists(path)
                if info.is_dir:
                    raise errors.FileExpected(path)

            s3file = S3File.factory(path, _mode, on_close=on_close)
            if _mode.appending:
                try:
                    with S3ClientErrors(path):
                        self.client.download_fileobj(self._bucket_name, _key, s3file.raw)
                except errors.ResourceNotFound:
                    pass
                else:
                    s3file.seek(0, os.SEEK_END)

            return s3file

        info = self.getinfo(path)
        if info.is_dir:
            raise errors.FileExpected(path)

        def on_close(s3file):
            try:
                if _mode.writing:
                    s3file.raw.seek(0, os.SEEK_SET)
                    self.client.upload_fileobj(s3file.raw, self._bucket_name, _key)
            finally:
                s3file.raw.close()

        s3file = S3File.factory(path, _mode, on_close=on_close)
        with S3ClientErrors(path):
            self.client.download_fileobj(self._bucket_name, _key, s3file.raw)
        s3file.seek(0, os.SEEK_SET)
        return s3file

    def remove(self, path):
        self.check()
        _path = self.validatepath(path)
        _key = self._path_to_key(_path)
        info = self.getinfo(path)
        if info.is_dir:
            raise errors.FileExpected(path)
        self.client.delete_object(
            Bucket=self._bucket_name,
            Key=_key
        )

    def isempty(self, path):
        self.check()
        _path = self.validatepath(path)
        _key = self._path_to_dir_key(_path)
        response = self.client.list_objects(
            Bucket=self._bucket_name,
            Prefix=_key,
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
        if _path == '/':
            raise errors.RemoveRootError()
        info = self.getinfo(_path)
        if not info.is_dir:
            raise errors.DirectoryExpected(path)
        if not self.isempty(path):
            raise errors.DirectoryNotEmpty(path)
        _key = self._path_to_dir_key(_path)
        self.client.delete_object(
            Bucket=self._bucket_name,
            Key=_key
        )

    def setinfo(self, path, info):
        self.getinfo(path)

    def getbytes(self, path):
        self.check()
        info = self.getinfo(path)
        if not info.is_file:
            raise errors.FileExpected(path)
        _path = self.validatepath(path)
        _key = self._path_to_key(_path)
        bytes_file = io.BytesIO()
        with S3ClientErrors(path):
            self.client.download_fileobj(self._bucket_name, _key, bytes_file)
        return bytes_file.getvalue()

    def exists(self, path):
        self.check()
        _path = self.validatepath(path)
        if _path == '/':
            return True
        _key = self._path_to_dir_key(_path)
        try:
            obj = self._get_object(path, _key)
        except errors.ResourceNotFound:
            return False
        else:
            return True

    def scandir(self, path, namespaces=None, page=None):
        _path = self.validatepath(path)
        namespaces = namespaces or ()
        _s3_key = self._path_to_dir_key(_path)
        prefix_len = len(_s3_key)

        info = self.getinfo(path)
        if not info.is_dir:
            raise errors.DirectoryExpected(path)

        paginator = self.client.get_paginator('list_objects')
        _paginate = paginator.paginate(
            Bucket=self._bucket_name,
            Prefix=_s3_key,
            Delimiter=self.delimiter
        )

        def gen_info():
            for result in _paginate:
                common_prefixes = result.get('CommonPrefixes', ())
                for prefix in common_prefixes:
                    _prefix = prefix.get('Prefix')
                    _name = _prefix[prefix_len:]
                    if _name != _s3_key:
                        info = {
                            "basic": {
                                "name": _name.rstrip(self.delimiter),
                                "is_dir": True
                            }
                        }
                        yield Info(info)
                for _obj in result.get('Contents', ()):
                    name = _obj["Key"][prefix_len:]
                    if name:
                        obj = self.s3.Object(self._bucket_name, _obj["Key"])
                        info = self._info_from_object(obj, namespaces)
                        yield Info(info)

        iter_info = iter(gen_info())
        if page is not None:
            start, end = page
            iter_info = itertools.islice(iter_info, start, end)

        for info in iter_info:
            yield info

    def setbytes(self, path, contents):
        if not isinstance(contents, bytes):
            raise ValueError('contents must be bytes')

        _path = self.validatepath(path)
        _key = self._path_to_key(_path)
        if not self.isdir(dirname(path)):
            raise errors.ResourceNotFound(path)
        try:
            info = self.getinfo(path)
            if info.is_dir:
                raise errors.FileExpected(path)
        except errors.ResourceNotFound:
            pass

        bytes_file = io.BytesIO(contents)
        with S3ClientErrors(path):
            self.client.upload_fileobj(bytes_file, self._bucket_name, _key)

    def setbinfile(self, path, file):
        _path = self.validatepath(path)
        _key = self._path_to_key(_path)
        if not self.isdir(dirname(path)):
            raise errors.ResourceNotFound(path)
        try:
            info = self.getinfo(path)
            if info.is_dir:
                raise errors.FileExpected(path)
        except errors.ResourceNotFound:
            pass

        with S3ClientErrors(path):
            self.client.upload_fileobj(file, self._bucket_name, _key)

    def copy(self, src_path, dst_path, overwrite=False):
        if not overwrite and self.exists(dst_path):
            raise errors.DestinationExists(dst_path)
        _src_path = self.validatepath(src_path)
        _dst_path = self.validatepath(dst_path)
        if not self.isdir(dirname(_dst_path)):
            raise errors.ResourceNotFound(dst_path)
        _src_key = self._path_to_key(_src_path)
        _dst_key = self._path_to_key(_dst_path)
        with S3ClientErrors(src_path):
            self.client.copy_object(
                Bucket=self._bucket_name,
                Key=_dst_key,
                CopySource={
                    'Bucket':self._bucket_name,
                    'Key':_src_key
                }
        )

    def move(self, src_path, dst_path, overwrite=False):
        self.copy(src_path, dst_path, overwrite=overwrite)
        self.remove(src_path)
