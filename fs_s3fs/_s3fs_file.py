# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Mariusz Kryński <mrk@sed.pl>
#           (C) 2019 Michael Penkov <m@penkov.dev>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
"""Implements file-like objects for reading and writing from/to S3."""

import io
from functools import wraps
import botocore.exceptions
import logging
import sys

logger = logging.getLogger(__name__)


def check_if_open(method):
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        if self.closed:
            logger.warning("file is already closed")
            return
        return method(self, *args, **kwargs)

    return wrapper


class S3InputFile(io.RawIOBase):
    def __init__(self, s3_object):
        self._s3_object = s3_object
        self._position = 0
        self._stream = None

    @property
    def size(self):
        if not hasattr(self, "_size"):
            self._size = self._s3_object.content_length
        return self._size

    @property
    def has_size(self):
        return hasattr(self, "_size")

    def _set_position(self, new_position):
        if new_position != self._position:
            if self._stream:
                self._stream.close()
                self._stream = None
            self._position = new_position

    def seek(self, offset, whence=io.SEEK_SET):
        if whence == io.SEEK_SET:
            self._set_position(offset)
        elif whence == io.SEEK_CUR:
            self._set_position(self._position + offset)
        elif whence == io.SEEK_END:
            if offset > 0:
                raise ValueError(
                    "invalid offset, for SEEK_END it should be less or equal 0"
                )
            self._set_position(self.size + offset)
        else:
            raise ValueError("invalid whence %r".format(whence))
        return self._position

    def read(self, size=-1):
        if size == 0 or self.has_size and self._position >= self.size:
            return b""

        if not self._stream:
            range_str = "bytes={}-".format(self._position)
            try:
                response = self._s3_object.get(Range=range_str)
            except botocore.exceptions.ClientError as e:
                error = e.response.get("Error", {})
                if error.get("Code") == "InvalidRange":
                    if "ActualObjectSize" in error:
                        self._size = int(error["ActualObjectSize"])
                    return b""
                raise
            content_range = response.get("ContentRange")
            if content_range:
                _, length = content_range.rsplit("/")
                self._size = int(length)
            self._stream = response["Body"]

        read_args = (size,) if size >= 0 else ()
        data = self._stream.read(*read_args)
        self._position += len(data)
        return data

    def readall(self):
        return self.read()

    def readinto(self, buf):
        data = self.read(len(buf))
        data_len = len(data)
        buf[:data_len] = data
        return data_len

    def close(self):
        if self._stream:
            self._stream.close()
            self._stream = None

    def readable(self):
        return True

    def seekable(self):
        return True


DEFAULT_MIN_PART_SIZE = 50 * 1024 ** 2
"""Default minimum part size for S3 multipart uploads"""

MIN_MIN_PART_SIZE = 5 * 1024 ** 2
"""The absolute minimum permitted by Amazon."""


class S3OutputFile(io.BufferedIOBase):
    """Writes bytes to S3.

    Implements the io.BufferedIOBase interface of the standard library."""

    def __init__(
        self,
        s3_object,
        min_part_size=DEFAULT_MIN_PART_SIZE,
        upload_kwargs=None,
    ):
        self._upload_kwargs = upload_kwargs or {}
        if min_part_size < MIN_MIN_PART_SIZE:
            logger.warning(
                "S3 requires minimum part size >= 5MB; multipart upload may fail"
            )

        self._object = s3_object
        self._min_part_size = min_part_size
        self._mp = None
        self._buf = bytearray()
        self._total_bytes = 0
        self._total_parts = 0
        self._parts = []
        self._closed = False

        #
        # This member is part of the io.BufferedIOBase interface.
        #
        self.raw = None

    def flush(self):
        pass

    @property
    def closed(self):
        return self._closed

    def writable(self):
        """Return True if the stream supports writing."""
        return True

    def tell(self):
        """Return the current stream position."""
        return self._total_bytes

    def detach(self):
        raise io.UnsupportedOperation("detach() not supported")

    @check_if_open
    def write(self, b):
        """Write the given buffer (bytes, bytearray, memoryview or any buffer
        interface implementation) to the S3 file.

        For more information about buffers, see
        https://docs.python.org/3/c-api/buffer.html

        There's buffering happening under the covers, so this may not actually
        do any HTTP transfer right away."""

        self._buf.extend(b)

        length = len(b)
        self._total_bytes += length

        if len(self._buf) >= self._min_part_size:
            self._upload_next_part()

        return length

    @check_if_open
    def close(self):
        logger.debug("closing")

        if tuple(sys.exc_info()) != (None, None, None):
            self.terminate()
            self._closed = True
            return

        if self._total_bytes < self._min_part_size:
            # if we wrote less than min_part_size bytes
            # then directly put buffer contents instead of starting
            # multipart upload. It also fixes following:
            #
            # AWS complains with "The XML you provided was not well-formed or
            # did not validate against our published schema" when the input is
            # completely empty => abort the upload, no file created.
            #
            assert not self._mp
            logger.debug("small input, ignoring multipart upload")
            self._object.put(Body=self._buf, **self._upload_kwargs)
        else:
            if self._buf:
                self._upload_next_part()
            self._mp.complete(MultipartUpload={"Parts": self._parts})
            logger.debug("completed multipart upload")

        self._mp = None
        self._closed = True
        logger.debug("successfully closed")

    @check_if_open
    def terminate(self):
        """Cancel the underlying multipart upload if any"""
        if self._mp:
            self._mp.abort()
            self._mp = None

    def _upload_next_part(self):
        if not self._mp:
            self._mp = self._object.initiate_multipart_upload(**self._upload_kwargs)
        part_num = self._total_parts + 1
        logger.info(
            "uploading part #%i, %i bytes (total %.3fGB)",
            part_num,
            len(self._buf),
            self._total_bytes / 1024.0 ** 3,
        )
        part = self._mp.Part(part_num)
        upload = part.upload(Body=self._buf)
        self._parts.append({"ETag": upload["ETag"], "PartNumber": part_num})
        logger.debug("upload of part #%i finished" % part_num)

        self._total_parts += 1
        self._buf.clear()
