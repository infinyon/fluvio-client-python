# Changelog

All notable changes to this project will be documented in this file.

## [0.15.7] - 2023-09-29

### Miscellaneous Tasks

- Bump setuptools-rust from 1.5.2 to 1.7.0 (#303)
- Bump fluvio-future from 0.5.1 to 0.6.0 (#304)
- Bump actions/checkout from 3 to 4 (#305)
- Bump black from 23.7.0 to 23.9.1 (#306)
- Bump cibuildwheel from 2.12.0 to 2.16.0 (#309)
- Bump pypa/cibuildwheel from 2.12.0 to 2.16.0 (#310)
- Update to fluvio 0.10.15
- Ci fix lates-dev-fluvio.yml workflow
- Release 0.15.7

## [0.15.6] - 2023-08-22

### Documentation

- Fix doc of Fluvio::connect (#301)

### Features

- Implement `Fluvio::connect_with_config`, add `FluvioConfig` (#300)

### Bump

- Bump version to prepare for release (#302)

## [0.15.5] - 2023-08-05

### Miscellaneous Tasks

- Update fluvio version 0.10.12-dev-1 (#273)

### Testing

- Add sleeps for CI realibility (#275)

### Bump

- Fluvio deps (#293)

## [0.15.4] - 2023-06-12

### Build

- Bump fluvio version (#266)
- Bump fluvio version (#267)

# Release Notes

## 0.13.0 - 2022-09-27
* Smart module filter ([#159](https://github.com/infinyon/fluvio-client-python/pull/159))
* Various dependency updates.

## 0.9.0 - 2021-08-05
* Removed `send_record` and `send_record_string` in place of `send` and
`send_string` as [`send_record` was removed in `0.8.0`](https://github.com/infinyon/fluvio/blob/master/CHANGELOG.md#platform-version-080---2021-04-27). ([#47](https://github.com/infinyon/fluvio-client-python/pull/47)).

## 0.8.1 - 2021-07-19
* Dependabot updates
* Update to use fluvio 0.8.5
* Universal mac builds for publishing to support Apple Silicon
