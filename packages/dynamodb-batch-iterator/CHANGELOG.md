# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [0.7.1]
Remove package rollup at `./build/index.mjs` due to bundler incompatibilities.

## [0.7.0]
Add a package rollup at `./build/index.mjs` to support tree shaking.

## [0.3.1]
### Fixed
 - When the source for a batch operation is a synchronous iterable, exhaust the
    source before interleaving throttled items.
 - When a write is returned as unprocessed, do not yield the marshalled form.

## [0.3.0]
Initial release
