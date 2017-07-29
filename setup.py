#!/usr/bin/env python

from setuptools import setup, find_packages

with open('s3fs/_version.py') as f:
    exec(f.read())

CLASSIFIERS = [
    'Development Status :: 5 - Production/Stable',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: MIT License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3.3',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'Topic :: System :: Filesystems',
]

with open('README.rst', 'rt') as f:
    DESCRIPTION = f.read()

REQUIREMENTS = [
    "boto3~=1.4.4",
    "fs~=2.0.4"
]

setup(
    author="Will McGugan",
    author_email="willmcgugan@gmail.com",
    classifiers=CLASSIFIERS,
    description="Amazon S3 filesystem for PyFilesystem",
    install_requires=REQUIREMENTS,
    license="MIT",
    long_description=DESCRIPTION,
    name='s3fs',
    packages=find_packages(exclude=("tests",)),
    platforms=['any'],
    test_suite="nose.collector",
    #tests_require=['appdirs', 'mock', 'pytz', 'pyftpdlib'],
    url="https://github.com/PyFilesystem/s3fs",
    version=__version__,
)
