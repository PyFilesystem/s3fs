from s3fs import S3FS
s3fs = S3FS(u'fsexample')

with s3fs.openbin(u'test.bin', u'w') as f:
    f.write(b'a')
    f.write(b'b')
    f.write(b'c')

print s3fs.getinfo(u'test.bin', namespaces=['s3']).raw

import io
f = io.BytesIO(b'Hello, World')
s3fs.setbinfile(u'b', f)



# f = s3fs.openbin(u'newfile', 'ab')
# f.write(b'Hello World !!!')
# f.close()

# f2 = s3fs.openbin(u'newfile')
# print(f2)
# print(f2.read())
# f2.close()