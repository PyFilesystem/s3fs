from fs_s3fs import S3FS
s3fs = S3FS(u'fsexample')
print(s3fs)

with s3fs.openbin(u'test.bin', u'w') as f:
    f.write(b'a')
    f.write(b'b')
    f.write(b'c')

print s3fs.getinfo(u'test.bin', namespaces=['s3']).raw

import io
f = io.BytesIO(b'Hello, World')
s3fs.setbinfile(u'b', f)

print(s3fs.geturl(u'b'))
s3fs.makedir(u'foo', recreate=True)
print(s3fs.geturl(u'/foo'))

s3fs.settext(u'/foo/bar', u'Hello')


s3fs = S3FS(u'fsexample', dir_path='foo')
print(s3fs)
print(s3fs._prefix)
print(s3fs.listdir(u'/'))
print(s3fs._path_to_dir_key(u'/'))
print(s3fs._path_to_dir_key(u''))
print(s3fs._path_to_dir_key(u'bar'))
print(s3fs._path_to_dir_key(u'/bar'))

# f = s3fs.openbin(u'newfile', 'ab')
# f.write(b'Hello World !!!')
# f.close()

# f2 = s3fs.openbin(u'newfile')
# print(f2)
# print(f2.read())
# f2.close()
