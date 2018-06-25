# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [0.1.9] - 2018-06-24

### Added

- upload_args and download_args to constructor (Geoff Jukes)

## [0.1.8] - 2018-03-29

### Changed

- Relaxed six dependency

## [0.1.7] - 2018-02-02

### Fixed

- Fix for opening file in missing directory

## [0.1.6] - 2018-01-31

### Added

- implemented new getfile method

### Changed

- Updated fs for more efficient directory walking
- Relaxed boto requirement

## [0.1.5] - 2017-10-21

### Added

- Added 'strict' parameter to constrictor.

## [0.1.4] - 2017-10-15

### Fixed

- copy() wasn't throwing FileExpected exception
- Added keys to s3 property
- Exception fixes in S3File

### Added

- Added endpoint_url to constructor

## [0.1.3] - 2017-09-01

### Fixed

- Issue with duplicate nested directory names.

### Changed

- Relaxed Boto requirement.

## [0.1.2] - 2017-08-29

### Fixed

- Issue with blank top level subdirectory.

## [0.1.1] - 2017-08-11

### Added

- Added new 'urls' namespace

## [0.1.0] - 2017-08-05

First official release
