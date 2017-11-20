# S3FS

S3FS is a [PyFilesystem](https://www.pyfilesystem.org/) interface to
Amazon S3 cloud storage.

As a PyFilesystem concrete class, [S3FS](http://fs-s3fs.readthedocs.io/en/latest/) allows you to work with S3 in the
same way as any other supported filesystem.

## Installing

You can install S3FS from pip as follows:

```
pip install fs-s3fs
```

## Opening a S3FS

Open an S3FS by explicitly using the constructor:

```python
from s3_s3fs import S3FS
s3fs = S3FS('mybucket')
```

Or with a FS URL:

```python
  from fs import open_fs
  s3fs = open_fs('s3://mybucket')
```

## Downloading Files

To *download* files from an S3 bucket, open a file on the S3
filesystem for reading, then write the data to a file on the local
filesystem. Here's an example that copies a file `example.mov` from
S3 to your HD:

```python
from fs.tools import copy_file_data
with s3fs.open('example.mov', 'rb') as remote_file:
    with open('example.mov', 'wb') as local_file:
        copy_file_data(remote_file, local_file)
```

Although it is preferable to use the higher-level functionality in the
`fs.copy` module. Here's an example:

```python
from fs.copy import copy_file
copy_file(s3fs, 'example.mov', './', 'example.mov')
```

## Uploading Files

You can *upload* files in the same way. Simply copy a file from a
source filesystem to the S3 filesystem.
See [Moving and Copying](https://docs.pyfilesystem.org/en/latest/guide.html#moving-and-copying)
for more information.

## S3 URLs

You can get a public URL to a file on a S3 bucket as follows:

```python
movie_url = s3fs.geturl('example.mov')
```

## Documentation

- [PyFilesystem Wiki](https://www.pyfilesystem.org)
- [S3FS Reference](http://fs-s3fs.readthedocs.io/en/latest/)
- [PyFilesystem Reference](https://docs.pyfilesystem.org/en/latest/reference/base.html)
