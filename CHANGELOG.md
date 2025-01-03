# Changelog

All notable changes to this project will be documented in this file.

## 0.18.1

### Add

- Add offset management integration to consumer stream (#541)

### Miscellaneous Tasks

- Update python docs with creating topic (#537)

## 0.18.0

### Add

- improve topic creation api (#528)

## 0.17.0

### Add

- add ConsumerConfigExt as a recommended way of starting a consumer (#511)

### Miscellaneous Tasks

- Refactor for better code modules & docs

## [0.16.5]

### Bug Fixes

- Produce output async_wait result misconversion always resulted in None

### Miscellaneous Tasks

- Python 3.8 EOL
- Update CHANGELOG.md
- README.md update
- Make lint refresh and fmt fixes

## [0.16.4] - 2024-10-11

### Miscellaneous Tasks

- Update to fluvio 0.12.0 (#495)
- Fix readme examples (#465)
- Bump pypa/gh-action-pypi-publish from 1.9.0 to 1.10.0 (#469)
- Update cc requirement from =1.1.12 to =1.1.15 (#467)

## [0.16.3] - 2024-08-21

### Bug Fixes

- Switch to rustls (#376)
- Wrap multiple-partition-consumer correctly
- Change to recommended dev build (#396)
- Bindgen dependency when cross compilation (#463)
- Publish build wheels (#464)

### Features

- Release GIL for most "run_block_on" calls
- Add python realization of ProduceOutput and RecordMetadata

### Miscellaneous Tasks

- Ci update outdated deps (#372)
- Use fixed version of cc (#379)
- Fluvio-cloud-ci fix profile cfg
- Update to fluvio 0.11.5
- Bump cargo.toml minor ver
- Bump black from 24.1.1 to 24.3.0 (#395)
- Bump setuptools-rust from 1.8.1 to 1.9.0 (#374)
- Bump cibuildwheel from 2.16.0 to 2.17.0 (#391)
- Bump pypa/cibuildwheel from 2.16.5 to 2.17.0 (#392)
- Update async-lock requirement from 2.4.0 to 3.3.0 (#387)
- Bump black from 24.3.0 to 24.4.0 (#401)
- Bump fluvio-types from v0.11.5 to v0.11.6 (#402)
- Update webbrowser requirement from 0.8.2 to 1.0.0 (#405)
- Update to fluvio 0.11.8
- Ci, publish, add toolchain
- Bump pip version
- Bump cibuildwheel from 2.18.1 to 2.19.0 (#428)
- Bump pypa/cibuildwheel from 2.18.1 to 2.19.0 (#427)
- Bump fluvio-types from v0.11.8 to v0.11.9 (#425)
- Update fluvio (#432)
- Update cc requirement from =1.0.83 to =1.1.0 (#433)
- Refresh docs
- Ci, push docs on tag event
- Bump black from 24.4.0 to 24.8.0 (#450)
- Bump setuptools-rust from 1.9.0 to 1.10.1 (#449)
- Bump cibuildwheel from 2.19.0 to 2.20.0 (#446)
- Bump pypa/cibuildwheel from 2.19.0 to 2.20.0 (#451)
- Fluvio update to 0.11.11 (#445)
- Ci cloud fix (#456)
- Update cc requirement from =1.1.5 to =1.1.11 (#459)
- Update cc requirement from =1.1.11 to =1.1.12 (#460)
- Use admin module instead of cli in tests (#457)

### Build

- Bump pypa/gh-action-pypi-publish from 1.8.11 to 1.9.0 (#431)
- Update cc requirement from =1.1.0 to =1.1.5 (#434)

## [0.16.1] [v0.16.2]

- publishing fix releases

## [0.16.0] - 2023-01-31

### Miscellaneous Tasks
- Release 0.16.0

### Bump
- Bump fluvio-types from v0.10.15 to v0.10.16 (#319)
- Bump toml from 0.7.6 to 0.8.1 (#312)
- Bump fluvio from v0.10.15 to v0.10.16 (#318)
- Bump black from 23.9.1 to 23.10.0 (#323)
- Bump rustix from 0.37.23 to 0.37.25 (#324)
- Bump tokio from 1.29.1 to 1.33.0 (#321)
- Bump setuptools-rust from 1.7.0 to 1.8.1 (#328)
- Bump fluvio-types from v0.10.16 to v0.10.17 (#330)
- Bump fluvio from v0.10.16 to v0.10.17 (#329)
- Bump black from 23.10.0 to 23.11.0 (#332)
- Bump fluvio-types from v0.10.17 to v0.11.0 (#336)
- Bump tokio from 1.33.0 to 1.34.0 (#333)
- Bump fluvio from v0.10.17 to v0.11.0 (#335)
- Bump url from 2.4.0 to 2.5.0 (#338)
- Bump actions/setup-python from 4 to 5 (#342)
- Bump actions/download-artifact from 3 to 4 (#347)
- Bump actions/upload-artifact from 3 to 4 (#348)
- Bump openssl from 0.10.55 to 0.10.60 (#341)
- Bump black from 23.11.0 to 23.12.0 (#346)
- Bump unsafe-libyaml from 0.2.9 to 0.2.10 (#350)
- Bump tokio from 1.34.0 to 1.35.1 (#349)
- Bump fluvio-types from v0.11.0 to v0.11.2 (#343)
- Bump fluvio from v0.11.0 to v0.11.3 (#353)
- Bump actions/cache from 3 to 4 (#355)
- Bump env_logger from 0.10.0 to 0.11.0 (#356)
- Bump black from 23.12.0 to 24.1.1 (#357)

### Testing
- CI enable python 3.12 (#360)

### Build
- changed bindings from rust-cpython to pyO3 (#359)

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
