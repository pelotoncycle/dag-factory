# Peloton Changelog
All notable changes made by Peloton org will be documented in this file.
## [0.18.5] - 2025-04-25
- BUGFIX: changed from directory to surface all import errors, not just errors from the last config to be processed
- Added cicd  command line interface for validating dag imports and dataset files

## [0.18.2] - 2025-03-19
- updated from directory error handling to pass each failed dag as a seperate task
- extended dag factory config error to include dag_id, error trace and tags

## [0.18.1] - 2025-03-05
- added support for DatasetOrTimeSchedule
- Timetable support for CronTriggerTimetable only as part of a DatasetOrTimeSchedule

## [0.18.0] - 2025-03-04
### Changed
- Merged v 0.22 from upstream Dag-factory repo. See CHANGELOG.md for full list of changes
- Notable updates from upstream:
    - Data aware Scheduling
    - Dynamic Tasks with expand and partial
    - Decorated tasks
    - Tasks topologically sorted before creation preventing dependency errors during building
    - Added yml-dag property to doc md. YML config now visible in airflow documentation
- Added enforce-global-datasets property to force all datasets to be defined in a config file at the root of the config repo.
- Disabled dynamic tasks for decorators due to current airflow bug causing invalid default params to be passed to mapped tasks
- Disabled Astronomer telemetry
- dag_id_prefix deprecated
- Peloton callback format refactored to match upstream (breaking change)
## [v0.17.1.post9] - 2023-11-16
### Changed
- Change from_directory() to adapt new yaml file name convention in Airflow repo

## [v0.17.1.post8] - 2023-11-14
### Changed
- Change from_directory() to adapt new Airflow Dag folder structure
- Automatic owner & tagging from config file path
- Make 'maintainer' optional in tags
- Add source code github link to DAG Docs