# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a
Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## v0.0.4 - 2019-11-22

### Changed

  * [#1160](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/1160): Changes
    the retry period from 168 to 24 attemps to mark quickly a task that fails.

## v0.0.3 - 2019-11-21

### Changed

  * [#1160](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/1160): Changes
    the `Apache Airflow` version from `1.10.2` to `1.10.5`.
    Set the default pool for DataFlowDirectRunnerOperator the Pool.DEFAULT_POOL_NAME.

### Added

  * [#1160](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/1160): Adds
    google-cloud-storage lirary to run test for DataFlowDirectRunnnerOperator.
    SlackWebHookOperator to send notifications when a task fails.

## v0.0.2 - 2019-08-05

### Added

  * [#1100](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/1100): Adds
    a FlexibleOperator that could change easily from BashOperator to
    KubernetesPodOperator and fixes the build issue with `tzlocal` lib.

### Changed

  * [#1100](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/1100): Changes
    Avoiding hardcore of pool for kubernetesPodOperator when instances a
    FlexibleOperator.


## v0.0.1 - 2019-01-22

### Added

  * [#968](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/968): Adds
    Splits the airflow extension from dataflow tools in pipe-tools
