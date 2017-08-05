.. _s3fs:

S3FS
====

Construct a `PyFilesystem interface <https://docs.pyfilesystem.org/en/latest/reference/base.html>`_ to an Amazon S3 'bucket'.

Here's a silly example::

    from fs_s3fs import S3FS
    s3fs = S3FS('mybucket')
    with s3fs.open('foo.txt', 'wt') as fh:
        fh.write('Writing a file to the cloud!')
    print(s3fs.listdir(u'/'))


S3FS Constructor
----------------

.. autoclass:: fs_s3fs.S3FS
    :members:
