# dag-factory

[![Github Actions](https://github.com/astronomer/dag-factory/actions/workflows/cicd.yaml/badge.svg?branch=main&event=push)](https://github.com/astronomer/dag-factory/actions?workflow=build)
[![Coverage](https://codecov.io/github/astronomer/dag-factory/coverage.svg?branch=master)](https://codecov.io/github/astronomer/dag-factory?branch=master)
[![PyPi](https://img.shields.io/pypi/v/dag-factory.svg)](https://pypi.org/project/dag-factory/)
[![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![Downloads](https://img.shields.io/pypi/dm/dag-factory.svg)](https://img.shields.io/pypi/dm/dag-factory)

<img alt=analytics referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=2bb92a5b-beb3-48cc-a722-79dda1089eda" />

Welcome to *dag-factory*! *dag-factory* is a library for [Apache Airflow®](https://airflow.apache.org) to construct DAGs
declaratively via configuration files.

The minimum requirements for **dag-factory** are:

- Python 3.8.0+
- [Apache Airflow®](https://airflow.apache.org) 2.0+

For a gentle introduction, please take a look at our [Quickstart Guide](https://astronomer.github.io/dag-factory/latest/getting-started/quick-start-airflow-standalone/). For more examples, please see the
[examples](/examples) folder.

- [Quickstart](https://astronomer.github.io/dag-factory/latest/getting-started/quick-start-astro-cli/)
- [Benefits](#benefits)
- [Features](#features)
  - [Dynamically Mapped Tasks](https://astronomer.github.io/dag-factory/latest/features/dynamic_tasks/)
  - [Multiple Configuration Files](#multiple-configuration-files)
  - [Callbacks](#callbacks)
  - [Custom Operators](#custom-operators)
- [Notes](#notes)
  - [HttpSensor (since 1.0.0)](#httpsensor-since-100)
- [Contributing](https://astronomer.github.io/dag-factory/latest/contributing/howto/)

## Benefits

- Construct DAGs without knowing Python
- Construct DAGs without learning Airflow primitives
- Avoid duplicative code
- Everyone loves YAML! ;)

## Features

### Multiple Configuration Files

If you want to split your DAG configuration into multiple files, you can do so by leveraging a suffix in the configuration file name.

```python
    from dagfactory import load_yaml_dags  # load relevant YAML files as airflow DAGs

    load_yaml_dags(globals_dict=globals(), suffix=['dag.yaml'])
```

### Custom Operators

**dag-factory** supports using custom operators. To leverage, set the path to the custom operator within the `operator` key in the configuration file. You can add any additional parameters that the custom operator requires.

```yaml
...
  tasks:
    begin:
      operator: airflow.operators.dummy_operator.DummyOperator
    make_bread_1:
      operator: customized.operators.breakfast_operators.MakeBreadOperator
      bread_type: 'Sourdough'
```

![custom_operators.png](img/custom_operators.png)

### Callbacks

**dag-factory** also supports using "callbacks" at the DAG, Task, and TaskGroup level. These callbacks can be defined in
a few different ways. The first points directly to a Python function that has been defined in the `include/callbacks.py`
file.

```yaml
example_dag1:
  on_failure_callback: include.callbacks.example_callback1
...
```

Here, the `on_success_callback` points to first a file, and then to a function name within that file. Notice that this
callback is defined using `default_args`, meaning this callback will be applied to all tasks.

```yaml
example_dag1:
  ...
  default_args:
    on_success_callback_file: /usr/local/airflow/include/callbacks.py
    on_success_callback_name: example_callback1
```

**dag-factory** users can also leverage provider-built tools when configuring callbacks. In this example, the
`send_slack_notification` function from the Slack provider is used to dispatch a message when a DAG failure occurs. This
function is passed to the `callback` key under `on_failure_callback`. This pattern allows for callback definitions to
take parameters (such as `text`, `channel`, and `username`, as shown here).

**Note that this functionality is currently only supported for `on_failure_callback`'s defined at the DAG-level, or in
`default_args`. Support for other callback types and Task/TaskGroup-level definitions are coming soon.**

```yaml
example_dag1:
  on_failure_callback:
    callback: airflow.providers.slack.notifications.slack.send_slack_notification
    slack_conn_id: example_slack_id
    text: |
      :red_circle: Task Failed.
      This task has failed and needs to be addressed.
      Please remediate this issue ASAP.
    channel: analytics-alerts
    username: Airflow
...
```

## Notes

### HttpSensor (since 1.0.0)

The package `airflow.providers.http.sensors.http` is available for Airflow 2.0+

The following example shows `response_check` logic in a python file:

```yaml
task_2:
  operator: airflow.providers.http.sensors.http.HttpSensor
  http_conn_id: 'test-http'
  method: 'GET'
  response_check_name: check_sensor
  response_check_file: /path/to/example1/http_conn.py
  dependencies: [task_1]
```

The `response_check` logic can also be provided as a lambda:

```yaml
task_2:
  operator: airflow.providers.http.sensors.http.HttpSensor
  http_conn_id: 'test-http'
  method: 'GET'
  response_check_lambda: 'lambda response: "ok" in response.text'
  dependencies: [task_1]
```

## License

To learn more about the terms and conditions for use, reproduction and distribution, read the [Apache License 2.0](https://github.com/astronomer/dag-factory/blob/main/LICENSE).

## Privacy Notice

This project follows [Astronomer's Privacy Policy](https://www.astronomer.io/privacy/).

For further information, [read this](https://github.com/astronomer/dag-factory/blob/main/PRIVACY_NOTICE.md)

## Security Policy

Check the project's [Security Policy](https://github.com/astronomer/dag-factory/blob/main/SECURITY.md) to learn
how to report security vulnerabilities in DAG Factory and how security issues reported to the DAG Factory
security team are handled.
