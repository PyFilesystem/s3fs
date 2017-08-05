.. S3FS documentation master file, created by
   sphinx-quickstart on Sat Aug  5 12:55:45 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

S3FS
====

S3FS is a `PyFilesystem interface
<https://docs.pyfilesystem.org/en/latest/reference/base.html>`_ to
Amazon S3 cloud storage.

As a PyFilesystem concrete class, S3FS allows you to work with S3 in the
same as any other supported filesystem.


Installing
==========

S3FS may be installed from pip with the following command::

    pip install fs-s3fs

This will install the most recent stable version.

Alternatively, if you want the cutting edge code, you can check out
the GitHub repos at https://github.com/pyfilesystem/s3fs


Opening an S3 Filesystem
========================

There are two options for constructing a :ref:`s3fs` instance. The simplest way
is with an *opener*, which is a simple URL like syntax. Here is an example::

    from fs import open_fs
    s3fs = S3FS('s3://mybucket/')

For more granular control, you may import the S3FS class and construct
it explicitly::

    from fs_s3fs import S3FS
    s3fs = S3FS('mybucket')


Authentication
==============

If you don't supply any credentials, then S3FS will use the access key
and secret key configured on your system. You may also specify when
creating the filesystem instance. Here's how you would do that with an
opener::

    s3fs = open_fs('s3://<access key>:<secret key>/mybucket')

Here's how you specify credentials with the constructor::

    s3fs = S3FS(
        'mybucket'
        aws_access_key_id=<access key>,
        aws_secret_access_key=<secret key>
    )

.. note::

    Amazon recommends against specifying credentials explicitly like
    this in production.


S3 Info
=======

You can retrieve S3 info via the ``s3`` namespace. Here's an example:

    >>> info = s.getinfo(u'foo', namespaces=['s3'])
    >>> info.raw['s3']
    {u'content_length': 3, u'restore': None, u'sse_customer_key_md5': None, u'content_language': None, u'replication_status': None, u'server_side_encryption': None, u'parts_count': None, u'sse_customer_algorithm': None, u'missing_meta': None, u'delete_marker': None, u'content_encoding': None, u'accept_ranges': 'bytes', u'cache_control': None, u'metadata': {}, u'request_charged': None, u'e_tag': '"37b51d194a7513e45b56f6524f2d51f2"', u'expires': None, u'version_id': None, u'last_modified': 1501935315, u'content_type': 'binary/octet-stream', u'website_redirect_location': None, u'ssekms_key_id': None, u'content_disposition': None, u'storage_class': None, u'expiration': None}


URLs
====

You can use the ``geturl`` method to generate an externally accessible
URL from an S3 object. Here's an example:

>>> s3fs.geturl('foo')
'https://fsexample.s3.amazonaws.com//foo?AWSAccessKeyId=AKIAIEZZDQU72WQP3JUA&Expires=1501939084&Signature=4rfDuqVgmvILjtTeYOJvyIXRMvs%3D'

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   s3fs.rst


More Information
================

See the `PyFilesystem Docs <https://docs.pyfilesystem.org>`_ for full
details.


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
