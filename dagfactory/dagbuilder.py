"""Module contains code for generating tasks and constructing a DAG"""
# pylint: disable=ungrouped-imports
import os
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Union, Optional

from airflow import DAG, configuration
from airflow.models import BaseOperator, Variable
from airflow.utils.module_loading import import_string
from packaging import version

try:
    from airflow.version import version as AIRFLOW_VERSION
except ImportError:
    from airflow import __version__ as AIRFLOW_VERSION

# python operators were moved in 2.4
try:
    from airflow.operators.python import BranchPythonOperator, PythonOperator, ShortCircuitOperator
except ImportError:
    from airflow.operators.python_operator import BranchPythonOperator, PythonOperator, ShortCircuitOperator


# http sensor was moved in 2.4
try:
    from airflow.providers.http.sensors.http import HttpSensor
except ImportError:
    from airflow.sensors.http_sensor import HttpSensor

# sql sensor was moved in 2.4
try:
    from airflow.sensors.sql_sensor import SqlSensor
except ImportError:
    from airflow.providers.common.sql.sensors.sql import SqlSensor

# k8s libraries are moved in v5.0.0
try:
    from airflow.providers.cncf.kubernetes import get_provider_info

    K8S_PROVIDER_VERSION = get_provider_info.get_provider_info()["versions"][0]
except ImportError:
    K8S_PROVIDER_VERSION = "0"

# kubernetes operator
from dagfactory.utils import merge_configs

try:
    if version.parse(K8S_PROVIDER_VERSION) < version.parse("5.0.0"):
        from airflow.kubernetes.pod import Port
        from airflow.kubernetes.volume_mount import VolumeMount
        from airflow.kubernetes.volume import Volume
        from airflow.kubernetes.pod_runtime_info_env import PodRuntimeInfoEnv
    else:
        from kubernetes.client.models import V1ContainerPort as Port
        from kubernetes.client.models import (
            V1EnvVar,
            V1EnvVarSource,
            V1ObjectFieldSelector,
            V1Volume,
        )
        from kubernetes.client.models import V1VolumeMount as VolumeMount
    from airflow.kubernetes.secret import Secret
    from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
        KubernetesPodOperator,
    )
except ImportError:
    from airflow.contrib.kubernetes.secret import Secret
    from airflow.contrib.kubernetes.pod import Port
    from airflow.contrib.kubernetes.volume_mount import VolumeMount
    from airflow.contrib.kubernetes.volume import Volume
    from airflow.contrib.kubernetes.pod_runtime_info_env import PodRuntimeInfoEnv
    from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from kubernetes.client.models import V1Container, V1Pod

from dagfactory import utils

# pylint: disable=ungrouped-imports,invalid-name
# Disabling pylint's ungrouped-imports warning because this is a
# conditional import and cannot be done within the import group above
# TaskGroup is introduced in Airflow 2.0.0
if version.parse(AIRFLOW_VERSION) >= version.parse("2.0.0"):
    from airflow.sensors.python import PythonSensor
    from airflow.utils.task_group import TaskGroup
else:
    TaskGroup = None
    PythonSensor = None
# pylint: disable=ungrouped-imports,invalid-name

# TimeTable is introduced in Airflow 2.2.0
if version.parse(AIRFLOW_VERSION) >= version.parse("2.2.0"):
    from airflow.timetables.base import Timetable
else:
    Timetable = None
# pylint: disable=ungrouped-imports,invalid-name

# these are params only used in the DAG factory, not in the tasks
SYSTEM_PARAMS: List[str] = ["operator", "dependencies", "task_group_name"]


class DagBuilder:
    """
    Generates tasks and a DAG from a config.

    :param dag_name: the name of the DAG
    :param dag_config: a dictionary containing configuration for the DAG
    :param default_config: a dictitionary containing defaults for all DAGs
        in the YAML file
    """

    def __init__(
        self, dag_name: str, dag_config: Dict[str, Any], default_config: Dict[str, Any]
    ) -> None:
        self.dag_name: str = dag_name
        self.dag_config: Dict[str, Any] = deepcopy(dag_config)
        self.default_config: Dict[str, Any] = deepcopy(default_config)

    # pylint: disable=too-many-branches
    def get_dag_params(self) -> Dict[str, Any]:
        """
        Merges default config with dag config, sets dag_id, and extropolates dag_start_date

        :returns: dict of dag parameters
        """
        try:
            dag_params: Dict[str, Any] = utils.merge_configs(
                self.dag_config, self.default_config
            )
        except Exception as err:
            raise Exception("Failed to merge config with default config") from err
        dag_params["dag_id"]: str = dag_params.get("dag_id_prefix", "") + self.dag_name

        if dag_params.get("task_groups") and version.parse(
            AIRFLOW_VERSION
        ) < version.parse("2.0.0"):
            raise Exception("`task_groups` key can only be used with Airflow 2.x.x")

        if (
            utils.check_dict_key(dag_params, "schedule_interval")
            and dag_params["schedule_interval"] == "None"
        ):
            dag_params["schedule_interval"] = None

        # Convert from 'dagrun_timeout_sec: int' to 'dagrun_timeout: timedelta'
        if utils.check_dict_key(dag_params, "dagrun_timeout_sec"):
            dag_params["dagrun_timeout"]: timedelta = timedelta(
                seconds=dag_params["dagrun_timeout_sec"]
            )
            del dag_params["dagrun_timeout_sec"]

        # Convert from 'end_date: Union[str, datetime, date]' to 'end_date: datetime'
        if utils.check_dict_key(dag_params["default_args"], "end_date"):
            dag_params["default_args"]["end_date"]: datetime = utils.get_datetime(
                date_value=dag_params["default_args"]["end_date"],
                timezone=dag_params["default_args"].get("timezone", "UTC"),
            )

        if utils.check_dict_key(dag_params["default_args"], "retry_delay_sec"):
            dag_params["default_args"]["retry_delay"]: timedelta = timedelta(
                seconds=dag_params["default_args"]["retry_delay_sec"]
            )
            del dag_params["default_args"]["retry_delay_sec"]

        if utils.check_dict_key(dag_params["default_args"], "sla_secs"):
            dag_params["default_args"]["sla"]: timedelta = timedelta(
                seconds=dag_params["default_args"]["sla_secs"]
            )
            del dag_params["default_args"]["sla_secs"]

        if utils.check_dict_key(dag_params["default_args"], "sla_miss_callback"):
            if isinstance(dag_params["default_args"]["sla_miss_callback"], str):
                dag_params["default_args"][
                    "sla_miss_callback"
                ]: Callable = import_string(
                    dag_params["default_args"]["sla_miss_callback"]
                )

        if utils.check_dict_key(dag_params["default_args"], "on_success_callback"):
            if isinstance(dag_params["default_args"]["on_success_callback"], str):
                dag_params["default_args"][
                    "on_success_callback"
                ]: Callable = import_string(
                    dag_params["default_args"]["on_success_callback"]
                )
                if utils.check_dict_key(dag_params["default_args"], "on_success_callback_kwargs"):
                    on_success_callback_kwargs = dag_params["default_args"]["on_success_callback_kwargs"]
                    dag_params["default_args"][
                        "on_success_callback"
                    ]: Callable = dag_params["default_args"]["on_success_callback"](**on_success_callback_kwargs)
                    del dag_params["default_args"]["on_success_callback_kwargs"]

        if utils.check_dict_key(dag_params["default_args"], "on_failure_callback"):
            if isinstance(dag_params["default_args"]["on_failure_callback"], str):
                dag_params["default_args"][
                    "on_failure_callback"
                ]: Callable = import_string(
                    dag_params["default_args"]["on_failure_callback"]
                )
                if utils.check_dict_key(dag_params["default_args"], "on_failure_callback_kwargs"):
                    on_failure_callback_kwargs = dag_params["default_args"]["on_failure_callback_kwargs"]
                    dag_params["default_args"][
                        "on_failure_callback"
                    ]: Callable = dag_params["default_args"]["on_failure_callback"](**on_failure_callback_kwargs)
                    del dag_params["default_args"]["on_failure_callback_kwargs"]

        if utils.check_dict_key(dag_params["default_args"], "on_retry_callback"):
            if isinstance(dag_params["default_args"]["on_retry_callback"], str):
                dag_params["default_args"][
                    "on_retry_callback"
                ]: Callable = import_string(
                    dag_params["default_args"]["on_retry_callback"]
                )

        if utils.check_dict_key(dag_params, "sla_miss_callback"):
            if isinstance(dag_params["sla_miss_callback"], str):
                dag_params["sla_miss_callback"]: Callable = import_string(
                    dag_params["sla_miss_callback"]
                )

        if utils.check_dict_key(dag_params, "on_success_callback"):
            if isinstance(dag_params["on_success_callback"], str):
                dag_params["on_success_callback"]: Callable = import_string(
                    dag_params["on_success_callback"]
                )
                if utils.check_dict_key(dag_params, "on_success_callback_kwargs"):
                    on_success_callback_kwargs = dag_params["on_success_callback_kwargs"]
                    dag_params["on_success_callback"]: Callable = dag_params["on_success_callback"](
                        **on_success_callback_kwargs
                    )
                    del dag_params["on_success_callback_kwargs"]

        if utils.check_dict_key(dag_params, "on_failure_callback"):
            if isinstance(dag_params["on_failure_callback"], str):
                dag_params["on_failure_callback"]: Callable = import_string(
                    dag_params["on_failure_callback"]
                )
                if utils.check_dict_key(dag_params, "on_failure_callback_kwargs"):
                    on_failure_callback_kwargs = dag_params["on_failure_callback_kwargs"]
                    dag_params["on_failure_callback"]: Callable = dag_params["on_failure_callback"](
                        **on_failure_callback_kwargs
                    )
                    del dag_params["on_failure_callback_kwargs"]

        if utils.check_dict_key(
            dag_params, "on_success_callback_name"
        ) and utils.check_dict_key(dag_params, "on_success_callback_file"):
            dag_params["on_success_callback"]: Callable = utils.get_python_callable(
                dag_params["on_success_callback_name"],
                dag_params["on_success_callback_file"],
            )

        if utils.check_dict_key(
            dag_params, "on_failure_callback_name"
        ) and utils.check_dict_key(dag_params, "on_failure_callback_file"):
            dag_params["on_failure_callback"]: Callable = utils.get_python_callable(
                dag_params["on_failure_callback_name"],
                dag_params["on_failure_callback_file"],
            )

        try:
            # ensure that default_args dictionary contains key "start_date"
            # with "datetime" value in specified timezone
            dag_params["default_args"]["start_date"]: datetime = utils.get_datetime(
                date_value=dag_params["default_args"]["start_date"],
                timezone=dag_params["default_args"].get("timezone", "UTC"),
            )
        except KeyError as err:
            raise Exception(f"{self.dag_name} config is missing start_date") from err
        return dag_params

    @staticmethod
    def make_timetable(timetable: str, timetable_params: Dict[str, Any]) -> Timetable:
        """
        Takes a custom timetable and params and creates an instance of that timetable.

        :returns instance of timetable object
        """
        try:
            # class is a Callable https://stackoverflow.com/a/34578836/3679900
            timetable_obj: Callable[..., Timetable] = import_string(timetable)
        except Exception as err:
            raise Exception(
                f"Failed to import timetable {timetable} due to: {err}"
            ) from err
        try:
            schedule: Timetable = timetable_obj(**timetable_params)
        except Exception as err:
            raise Exception(f"Failed to create {timetable_obj} due to: {err}") from err
        return schedule

    # pylint: disable=too-many-branches
    # pylint: disable=too-many-statements
    @staticmethod
    def make_task(operator: str, task_params: Dict[str, Any]) -> BaseOperator:
        """
        Takes an operator and params and creates an instance of that operator.

        :returns: instance of operator object
        """
        try:
            # class is a Callable https://stackoverflow.com/a/34578836/3679900
            operator_obj: Callable[..., BaseOperator] = import_string(operator)
        except Exception as err:
            raise Exception(f"Failed to import operator: {operator}") from err
        # pylint: disable=too-many-nested-blocks
        try:
            if (operator_obj in [PythonOperator, BranchPythonOperator, PythonSensor, ShortCircuitOperator]
                    or issubclass(operator_obj, PythonOperator)
                    or issubclass(operator_obj, PythonSensor)):
                if (
                    not task_params.get("python_callable")
                    and not task_params.get("python_callable_name")
                    and not task_params.get("python_callable_file")
                ):
                    # pylint: disable=line-too-long
                    raise Exception(
                        "Failed to create task. PythonOperator, BranchPythonOperator, PythonSensor and ShortCircuitOperator requires \
                        `python_callable_name` and `python_callable_file` "
                        "parameters.\nOptionally you can load python_callable "
                        "from a file. with the special pyyaml notation:\n"
                        "  python_callable_file: !!python/name:my_module.my_func"
                    )
                if not task_params.get("python_callable"):
                    task_params[
                        "python_callable"
                    ]: Callable = utils.get_python_callable(
                        task_params["python_callable_name"],
                        task_params["python_callable_file"],
                    )
                    # remove dag-factory specific parameters
                    # Airflow 2.0 doesn't allow these to be passed to operator
                    del task_params["python_callable_name"]
                    del task_params["python_callable_file"]

            # Check for the custom success and failure callables in SqlSensor. These are considered
            # optional, so no failures in case they aren't found. Note: there's no reason to
            # declare both a callable file and a lambda function for success/failure parameter.
            # If both are found the object will not throw and error, instead callable file will
            # take precedence over the lambda function
            if operator_obj in [SqlSensor]:
                # Success checks
                if task_params.get("success_check_file") and task_params.get(
                    "success_check_name"
                ):
                    task_params["success"]: Callable = utils.get_python_callable(
                        task_params["success_check_name"],
                        task_params["success_check_file"],
                    )
                    del task_params["success_check_name"]
                    del task_params["success_check_file"]
                elif task_params.get("success_check_lambda"):
                    task_params["success"]: Callable = utils.get_python_callable_lambda(
                        task_params["success_check_lambda"]
                    )
                    del task_params["success_check_lambda"]
                # Failure checks
                if task_params.get("failure_check_file") and task_params.get(
                    "failure_check_name"
                ):
                    task_params["failure"]: Callable = utils.get_python_callable(
                        task_params["failure_check_name"],
                        task_params["failure_check_file"],
                    )
                    del task_params["failure_check_name"]
                    del task_params["failure_check_file"]
                elif task_params.get("failure_check_lambda"):
                    task_params["failure"]: Callable = utils.get_python_callable_lambda(
                        task_params["failure_check_lambda"]
                    )
                    del task_params["failure_check_lambda"]

            if operator_obj in [HttpSensor]:
                if not (
                    task_params.get("response_check_name")
                    and task_params.get("response_check_file")
                ) and not task_params.get("response_check_lambda"):
                    raise Exception(
                        "Failed to create task. HttpSensor requires \
                        `response_check_name` and `response_check_file` parameters \
                        or `response_check_lambda` parameter."
                    )
                if task_params.get("response_check_file"):
                    task_params["response_check"]: Callable = utils.get_python_callable(
                        task_params["response_check_name"],
                        task_params["response_check_file"],
                    )
                    # remove dag-factory specific parameters
                    # Airflow 2.0 doesn't allow these to be passed to operator
                    del task_params["response_check_name"]
                    del task_params["response_check_file"]
                else:
                    task_params[
                        "response_check"
                    ]: Callable = utils.get_python_callable_lambda(
                        task_params["response_check_lambda"]
                    )
                    # remove dag-factory specific parameters
                    # Airflow 2.0 doesn't allow these to be passed to operator
                    del task_params["response_check_lambda"]

            # KubernetesPodOperator
            if operator_obj == KubernetesPodOperator:
                task_params["secrets"] = (
                    [Secret(**v) for v in task_params.get("secrets")]
                    if task_params.get("secrets") is not None
                    else None
                )

                task_params["ports"] = (
                    [Port(**v) for v in task_params.get("ports")]
                    if task_params.get("ports") is not None
                    else None
                )
                task_params["volume_mounts"] = (
                    [VolumeMount(**v) for v in task_params.get("volume_mounts")]
                    if task_params.get("volume_mounts") is not None
                    else None
                )
                if version.parse(K8S_PROVIDER_VERSION) < version.parse("5.0.0"):
                    task_params["volumes"] = (
                        [Volume(**v) for v in task_params.get("volumes")]
                        if task_params.get("volumes") is not None
                        else None
                    )
                    task_params["pod_runtime_info_envs"] = (
                        [
                            PodRuntimeInfoEnv(**v)
                            for v in task_params.get("pod_runtime_info_envs")
                        ]
                        if task_params.get("pod_runtime_info_envs") is not None
                        else None
                    )
                else:
                    if task_params.get("volumes") is not None:
                        task_params_volumes = []
                        for vol in task_params.get("volumes"):
                            resp = V1Volume(name=vol.get("name"))
                            for k, v in vol["configs"].items():
                                snake_key = utils.convert_to_snake_case(k)
                                if hasattr(resp, snake_key):
                                    setattr(resp, snake_key, v)
                                else:
                                    raise Exception(
                                        f"Volume for KubernetesPodOperator \
                                        does not have attribute {k}"
                                    )
                            task_params_volumes.append(resp)
                        task_params["volumes"] = task_params_volumes
                    else:
                        task_params["volumes"] = None

                    task_params["pod_runtime_info_envs"] = (
                        [
                            V1EnvVar(
                                name=v.get("name"),
                                value_from=V1EnvVarSource(
                                    field_ref=V1ObjectFieldSelector(
                                        field_path=v.get("field_path")
                                    )
                                ),
                            )
                            for v in task_params.get("pod_runtime_info_envs")
                        ]
                        if task_params.get("pod_runtime_info_envs") is not None
                        else None
                    )
                task_params["full_pod_spec"] = (
                    V1Pod(**task_params.get("full_pod_spec"))
                    if task_params.get("full_pod_spec") is not None
                    else None
                )
                task_params["init_containers"] = (
                    [V1Container(**v) for v in task_params.get("init_containers")]
                    if task_params.get("init_containers") is not None
                    else None
                )

            if utils.check_dict_key(task_params, "execution_timeout_secs"):
                task_params["execution_timeout"]: timedelta = timedelta(
                    seconds=task_params["execution_timeout_secs"]
                )
                del task_params["execution_timeout_secs"]

            if utils.check_dict_key(task_params, "sla_secs"):
                task_params["sla"]: timedelta = timedelta(
                    seconds=task_params["sla_secs"]
                )
                del task_params["sla_secs"]

            if utils.check_dict_key(task_params, "execution_delta_secs"):
                task_params["execution_delta"]: timedelta = timedelta(
                    seconds=task_params["execution_delta_secs"]
                )
                del task_params["execution_delta_secs"]

            if utils.check_dict_key(
                task_params, "execution_date_fn_name"
            ) and utils.check_dict_key(task_params, "execution_date_fn_file"):
                task_params["execution_date_fn"]: Callable = utils.get_python_callable(
                    task_params["execution_date_fn_name"],
                    task_params["execution_date_fn_file"],
                )
                del task_params["execution_date_fn_name"]
                del task_params["execution_date_fn_file"]

            # on_execute_callback is an Airflow 2.0 feature
            if utils.check_dict_key(
                task_params, "on_execute_callback"
            ) and version.parse(AIRFLOW_VERSION) >= version.parse("2.0.0"):
                task_params["on_execute_callback"]: Callable = import_string(
                    task_params["on_execute_callback"]
                )

            if utils.check_dict_key(task_params, "on_failure_callback"):
                task_params["on_failure_callback"]: Callable = import_string(
                    task_params["on_failure_callback"]
                )
                if utils.check_dict_key(task_params, "on_failure_callback_kwargs"):
                    on_failure_callback_kwargs = task_params["on_failure_callback_kwargs"]
                    task_params["on_failure_callback"]: Callable = task_params["on_failure_callback"](
                        **on_failure_callback_kwargs)
                    del task_params["on_failure_callback_kwargs"]

            if utils.check_dict_key(task_params, "on_success_callback"):
                task_params["on_success_callback"]: Callable = import_string(
                    task_params["on_success_callback"]
                )
                if utils.check_dict_key(task_params, "on_success_callback_kwargs"):
                    on_success_callback_kwargs = task_params["on_success_callback_kwargs"]
                    task_params["on_success_callback"]: Callable = task_params["on_success_callback"](
                        **on_success_callback_kwargs
                    )
                    del task_params["on_success_callback_kwargs"]

            if utils.check_dict_key(task_params, "on_retry_callback"):
                task_params["on_retry_callback"]: Callable = import_string(
                    task_params["on_retry_callback"]
                )

            # use variables as arguments on operator
            if utils.check_dict_key(task_params, "variables_as_arguments"):
                variables: List[Dict[str, str]] = task_params.get(
                    "variables_as_arguments"
                )
                for variable in variables:
                    if Variable.get(variable["variable"], default_var=None) is not None:
                        task_params[variable["attribute"]] = Variable.get(
                            variable["variable"], default_var=None
                        )
                del task_params["variables_as_arguments"]

            task: BaseOperator = operator_obj(**task_params)
        except Exception as err:
            raise Exception(f"Failed to create {operator_obj} task") from err
        return task

    @staticmethod
    def make_task_groups(
        task_groups: Dict[str, Any], dag: DAG
    ) -> Dict[str, "TaskGroup"]:
        """Takes a DAG and task group configurations. Creates TaskGroup instances.

        :param task_groups: Task group configuration from the YAML configuration file.
        :param dag: DAG instance that task groups to be added.
        """
        task_groups_dict: Dict[str, "TaskGroup"] = {}
        if version.parse(AIRFLOW_VERSION) >= version.parse("2.0.0"):
            for task_group_name, task_group_conf in task_groups.items():
                task_group_conf["group_id"] = task_group_name
                task_group_conf["dag"] = dag
                task_group = TaskGroup(
                    **{
                        k: v
                        for k, v in task_group_conf.items()
                        if k not in SYSTEM_PARAMS
                    }
                )
                task_groups_dict[task_group.group_id] = task_group
        return task_groups_dict

    @staticmethod
    def set_dependencies(
        tasks_config: Dict[str, Dict[str, Any]],
        operators_dict: Dict[str, BaseOperator],
        task_groups_config: Dict[str, Dict[str, Any]],
        task_groups_dict: Dict[str, "TaskGroup"],
    ):
        """Take the task configurations in YAML file and operator
        instances, then set the dependencies between tasks.

        :param tasks_config: Raw task configuration from YAML file
        :param operators_dict: Dictionary for operator instances
        :param task_groups_config: Raw task group configuration from YAML file
        :param task_groups_dict: Dictionary for task group instances
        """
        tasks_and_task_groups_config = {**tasks_config, **task_groups_config}
        tasks_and_task_groups_instances = {**operators_dict, **task_groups_dict}
        for name, conf in tasks_and_task_groups_config.items():
            # if task is in a task group, group_id is prepended to its name
            if conf.get("task_group"):
                group_id = conf["task_group"].group_id
                name = f"{group_id}.{name}"
            if conf.get("dependencies"):
                source: Union[
                    BaseOperator, "TaskGroup"
                ] = tasks_and_task_groups_instances[name]
                for dep in conf["dependencies"]:
                    if tasks_and_task_groups_config[dep].get("task_group"):
                        group_id = tasks_and_task_groups_config[dep][
                            "task_group"
                        ].group_id
                        dep = f"{group_id}.{dep}"
                    dep: Union[
                        BaseOperator, "TaskGroup"
                    ] = tasks_and_task_groups_instances[dep]
                    source.set_upstream(dep)

    def build(self) -> Dict[str, Union[str, DAG]]:
        """
        Generates a DAG from the DAG parameters.

        :returns: dict with dag_id and DAG object
        :type: Dict[str, Union[str, DAG]]
        """
        dag_params: Dict[str, Any] = self.get_dag_params()
        env_var_name = dag_params.get("env_var_name", "NOT_SPECIFIED")

        # skip importing if the env is wrong
        if "allowed_envs" in dag_params:
            if os.environ.get("ENV", os.environ.get(env_var_name)) not in dag_params["allowed_envs"]:
                return {"dag_id": dag_params["dag_id"], "dag": "wrong env, import skipped"}

        # skip importing if the namespace is wrong
        if "allowed_namespaces" in dag_params:
            if os.environ.get("AIRFLOW__KUBERNETES__NAMESPACE") not in dag_params["allowed_namespaces"]:
                return {"dag_id": dag_params["dag_id"], "dag": "wrong namespace, import skipped"}

        dag_kwargs: Dict[str, Any] = {}

        dag_kwargs["dag_id"] = dag_params["dag_id"]

        if not dag_params.get("timetable"):
            dag_kwargs["schedule_interval"] = dag_params.get(
                "schedule_interval", timedelta(days=1)
            )

        if version.parse(AIRFLOW_VERSION) >= version.parse("1.10.11"):
            dag_kwargs["description"] = dag_params.get("description", None)
        else:
            dag_kwargs["description"] = dag_params.get("description", "")

        if version.parse(AIRFLOW_VERSION) >= version.parse("2.2.0"):
            dag_kwargs["max_active_tasks"] = dag_params.get(
                "max_active_tasks",
                configuration.conf.getint("core", "max_active_tasks_per_dag"),
            )

            if dag_params.get("timetable"):
                timetable_args = dag_params.get("timetable")
                dag_kwargs["timetable"] = DagBuilder.make_timetable(
                    timetable_args.get("callable"), timetable_args.get("params")
                )
        else:
            dag_kwargs["concurrency"] = dag_params.get(
                "concurrency", configuration.conf.getint("core", "dag_concurrency")
            )

        dag_kwargs["catchup"] = dag_params.get(
            "catchup", configuration.conf.getboolean("scheduler", "catchup_by_default")
        )

        dag_kwargs["max_active_runs"] = dag_params.get(
            "max_active_runs",
            configuration.conf.getint("core", "max_active_runs_per_dag"),
        )

        dag_kwargs["dagrun_timeout"] = dag_params.get("dagrun_timeout", None)

        dag_kwargs["default_view"] = dag_params.get(
            "default_view", configuration.conf.get("webserver", "dag_default_view")
        )

        dag_kwargs["orientation"] = dag_params.get(
            "orientation", configuration.conf.get("webserver", "dag_orientation")
        )

        dag_kwargs["sla_miss_callback"] = dag_params.get("sla_miss_callback", None)

        dag_kwargs["on_success_callback"] = dag_params.get("on_success_callback", None)

        dag_kwargs["on_failure_callback"] = dag_params.get("on_failure_callback", None)

        dag_kwargs["default_args"] = dag_params.get("default_args", None)

        dag_kwargs["doc_md"] = dag_params.get("doc_md", None)

        dag_kwargs["template_searchpath"] = dag_params.get("template_searchpath", None)

        dag_kwargs["access_control"] = dag_params.get("access_control", None)

        dag_kwargs["is_paused_upon_creation"] = dag_params.get(
            "is_paused_upon_creation", None
        )

        dag_kwargs["params"] = dag_params.get("params", None)

        dag_class: Callable = DAG
        if utils.check_dict_key(dag_params, "dag_class"):
            dag_class: Callable = import_string(
                dag_params["dag_class"]
            )

        if dag_class.__name__ == "PelotonDepDag":
            dag_kwargs["depend_on_dags"] = dag_params.get("depend_on_dags", None)
            dag_kwargs["wait_on_dags"] = dag_params.get("wait_on_dags", None)
            dag_kwargs["depend_on_tasks"] = dag_params.get("depend_on_tasks", None)
            dag_kwargs["wait_on_tasks"] = dag_params.get("wait_on_tasks", None)
            dag_kwargs["alert_on_start"] = dag_params.get("alert_on_start", None)
            dag_kwargs["alert_on_finish"] = dag_params.get("alert_on_finish", None)

        operator_defaults: Optional[Dict] = None
        if utils.check_dict_key(dag_params, "operator_defaults"):
            operator_defaults = dag_params["operator_defaults"]

        with dag_class(**dag_kwargs) as dag:

            if dag_params.get("doc_md_file_path"):
                if not os.path.isabs(dag_params.get("doc_md_file_path")):
                    raise Exception("`doc_md_file_path` must be absolute path")

                with open(
                    dag_params.get("doc_md_file_path"), "r", encoding="utf-8"
                ) as file:
                    dag.doc_md = file.read()

            if dag_params.get("doc_md_python_callable_file") and dag_params.get(
                "doc_md_python_callable_name"
            ):
                doc_md_callable = utils.get_python_callable(
                    dag_params.get("doc_md_python_callable_name"),
                    dag_params.get("doc_md_python_callable_file"),
                )
                dag.doc_md = doc_md_callable(
                    **dag_params.get("doc_md_python_arguments", {})
                )

            # tags parameter introduced in Airflow 1.10.8
            if version.parse(AIRFLOW_VERSION) >= version.parse("1.10.8"):
                dag.tags = dag_params.get("tags", None)

            tasks: Dict[str, Dict[str, Any]] = dag_params["tasks"]

            # add a property to mark this dag as an auto-generated on
            dag.is_dagfactory_auto_generated = True

            # create dictionary of task groups
            task_groups_dict: Dict[str, "TaskGroup"] = self.make_task_groups(
                dag_params.get("task_groups", {}), dag
            )

            # create dictionary to track tasks and set dependencies
            tasks_dict: Dict[str, BaseOperator] = {}
            for task_name, task_conf in tasks.items():
                task_conf["task_id"]: str = task_name
                operator: str = task_conf["operator"]
                task_conf["dag"]: DAG = dag
                # add task to task_group
                if task_groups_dict and task_conf.get("task_group_name"):
                    task_conf["task_group"] = task_groups_dict[
                        task_conf.get("task_group_name")
                    ]
                # merge task configs with global task configs if there's any
                if operator_defaults and operator in set(operator_defaults.keys()):
                    task_conf = merge_configs(task_conf, operator_defaults[operator])

                params: Dict[str, Any] = {
                    k: v for k, v in task_conf.items() if k not in SYSTEM_PARAMS
                }
                task: BaseOperator = DagBuilder.make_task(
                    operator=operator, task_params=params
                )
                tasks_dict[task.task_id]: BaseOperator = task

            # set task dependencies after creating tasks
            self.set_dependencies(
                tasks, tasks_dict, dag_params.get("task_groups", {}), task_groups_dict
            )

        return {"dag_id": dag_params["dag_id"], "dag": dag}
