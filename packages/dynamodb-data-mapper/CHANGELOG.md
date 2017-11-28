# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Removed
 - **BREAKING CHANGE**: Removed the `returnValues` parameter from `put`. `put`
    will now always return the value that was persisted, thereby providing
    access to injected defaults and accurate version numbers.

### Added
 - Add a `parallelScan` method to the DataMapper.
 - Add optional parameters to the `scan` method to allow its use as a parallel
    scan worker
 - Add a `pageSize` parameter to `query` and `scan` to limit the size of pages
    fetched during a read. `pageSize` was previously called `limit`.

### Changed
 - Use TSLib instead of having TypeScript generate helpers to reduce bundle size

### Deprecated
 - Deprecate `limit` parameter on `query` and `scan`. It has been renamed to
    `pageSize`, though a value provided for `limit` will still be used if no
    `pageSize` parameter is provided.

## [0.1.1]
### Fixed
 - Update dependency version to match released version identifier

## [0.1.0]
Initial release
