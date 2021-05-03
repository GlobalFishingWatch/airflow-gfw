# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a
Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## v1.3.0 - 2021-04-27

### Added

  * [PIPELINE-84](https://globalfishingwatch.atlassian.net/browse/PIPELINE-84): Adds
    the fix to parse correctly the url
    (`https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.locations.jobs/get`)
    to get the Dataflow job id and let knowing the status of the DF job from
    Airflow side for function `wait_for_done()`.
    Pin sqlalchemy version to `1.3.24` to avoid breaking tests.

## v1.2.0 - 2020-12-15

### Changed

  * [PIPELINE-239](https://globalfishingwatch.atlassian.net/browse/PIPELINE-239): Changes
    the version of `Apache Airflow` to `1.10.14`.
    Updates libs `kubernetes:12.0.1` and `pandas_gbq:0.14.1`.
    Removes unused libs.

## v1.1.0 - 2020-11-13

### Added

  * [PIPELINE-155](https://globalfishingwatch.atlassian.net/browse/PIPELINE-155):
    Adds a new method to obtain the nodash version of `source_date_range`,
    useful when dealing with date-sharded bq inputs

  * [PIPELINE-241](https://globalfishingwatch.atlassian.net/browse/PIPELINE-241):
    Adds pin over `marshmallow-sqlalchemy` and `marshmallow` packages to create image without errors.
    Also fixes travis and add build reference in the `README.md`.

## v1.0.4 - 2020-08-04

### Removed

  * [GlobalFishingWatch/gfw-eng-tasks#143](https://github.com/GlobalFishingWatch/gfw-eng-tasks/issues/143): Removes
    invalid arguments (`proejct_id` and `dataset_id`) passed to `BigQueryCheckOperator`.

## v1.0.3 - 2020-06-05

### Added

  * [GlobalFishingWatch/gfw-eng-tasks#105](https://github.com/GlobalFishingWatch/gfw-eng-tasks/issues/105): Adds
    table partition check when using `check_tables` or `check_table` in Airflow Variables.

## v1.0.2 - 2020-04-24

### Changed

  * [GlobalFishingWatch/gfw-eng-tasks#51](https://github.com/GlobalFishingWatch/gfw-eng-tasks/issues/51): Changes
    use of Apache Airflow from `1.10.5` to `1.10.10`.

## v1.0.1 - 2020-03-31

### Added

  * [GlobalFishingWatch/gfw-eng-tasks#42](https://github.com/GlobalFishingWatch/gfw-eng-tasks/issues/42): Adds
    support to setup timeout and retries for gcs sensor.

## v1.0.0 - 2020-03-09

### Added

  * [GlobalFishingWatch/gfw-eng-tasks#23](https://github.com/GlobalFishingWatch/gfw-eng-tasks/issues/23): Adds
    support of python 3.7

## v0.0.7 - 2020-01-22

### Added

  * [#1178](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/1178): Adds
    missing return of the FlexibleOperator operator.

## v0.0.6 - 2019-12-19

### Changed

  * [#1164](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/1164): Changes
    turninng off the retries process for gcs_sensor.

## v0.0.5 - 2019-12-05

### Added

  * [#1164](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/1164): Added
    gcs sensor on model of DAG for airflow-gfw.

### Added

  * [#1165](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/1165): Adds
    a new method to instantiate a docker task directly without going through
    the `FlexibleOperator`.

## v0.0.4 - 2019-11-22

### Changed

  * [#1160](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/1160): Changes
    on how the sensor of tables operates:
    * Uses mode reschedule, the sensor will free the work slot.
    * Increases the poke interval to 10 minutes.
    * Increases the timeout to 24hs.

### Removed

  * [#1160](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/1160): Removes
    the `retries` parameter, `retry_delay`, `retry_exponential_backoff`.

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
