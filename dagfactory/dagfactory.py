"""Module contains code for loading a DagFactory config and generating DAGs"""
import datetime
import traceback
import logging
import re
import os
from itertools import chain
from pathlib import Path
from typing import Any, Dict, Set, List, Optional, Union

import pendulum
import yaml
import json
from airflow.configuration import conf as airflow_conf
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator

from dagfactory.dagbuilder import DagBuilder
from dagfactory.exceptions import DagFactoryConfigException, DagFactoryException

# these are params that cannot be a dag name
from dagfactory.utils import merge_configs

SYSTEM_PARAMS: List[str] = ["default", "task_groups"]
ALLOWED_CONFIG_FILE_SUFFIX: List[str] = ["yaml", "yml"]

logger = logging.getLogger(__file__)

class DagFactory:
    """
    Takes a YAML config or a python dictionary and generates DAGs.

    :param config_filepath: the filepath of the DAG factory YAML config file.
        Must be absolute path to file. Cannot be used with `config`.
    :type config_filepath: str
    :param config: DAG factory config dictionary. Cannot be user with `config_filepath`.
    :type config: dict
    """

    DAGBAG_IMPORT_ERROR_TRACEBACKS = airflow_conf.getboolean('core', 'dagbag_import_error_tracebacks')
    DAGBAG_IMPORT_ERROR_TRACEBACK_DEPTH = airflow_conf.getint('core', 'dagbag_import_error_traceback_depth')

    def __init__(
            self,
            config_filepath: Optional[str] = None,
            default_config: Optional[dict] = None,
            config: Optional[dict] = None,
            enforce_global_datasets: Optional[bool] = True
    ) -> None:
        assert bool(config_filepath) ^ bool(
            config
        ), "Either `config_filepath` or `config` should be provided"
        self.default_config = default_config
        if config_filepath:
            DagFactory._validate_config_filepath(config_filepath=config_filepath)
            self.config: Dict[str, Any] = DagFactory._load_config(
                config_filepath=config_filepath
            )
            dag_id = list(self.config.keys())[0]
            if 'git/repo' in config_filepath:
                git_filepath = config_filepath.replace('/git/repo', 'https://github.com/pelotoncycle/data-engineering-airflow-dags/tree/master')
            elif 'ds-dbt' in config_filepath:
                git_filepath = config_filepath.replace('/dbt-airflow/ds-dbt', 'https://github.com/pelotoncycle/ds-dbt/tree/master')
            elif 'cdw-dbt' in config_filepath:
                git_filepath = config_filepath.replace('/dbt-airflow/cdw-dbt', 'https://github.com/pelotoncycle/cdw-dbt/tree/master')
            elif 'data-engineering-dbt' in config_filepath:
                git_filepath = config_filepath.replace('/dbt-airflow/data-engineering-dbt', 'https://github.com/pelotoncycle/data-engineering-dbt/tree/master')
            else:
                git_filepath = config_filepath
            file_loc = f'Code: [Github Link]({git_filepath}).'
            if 'doc_md' in self.config[dag_id]:
                self.config[dag_id]['doc_md'] = ''.join([self.config[dag_id]['doc_md'], file_loc])
            else:
                self.config[dag_id]['doc_md'] = file_loc
        if config:
            self.config: Dict[str, Any] = config
        self.enforce_global_datasets: bool = enforce_global_datasets



    @classmethod
    def _from_directory(
        cls, 
        config_dir, 
        globals: Dict[str, Any], 
        parent_default_config: Optional[Dict[str, Any]] = None, 
        root_level: Optional[bool] = True, 
        config_filter: Optional[Set[str]]=None,
        ):
        """
        Make instances of DagFactory for each yaml configuration files within a directory
        """
        cls._validate_config_filepath(config_dir)
        subs = os.listdir(config_dir)

        # get default configurations if exist
        allowed_default_filename = ['default.' + sfx for sfx in ALLOWED_CONFIG_FILE_SUFFIX]
        maybe_default_file = [sub for sub in subs if sub in allowed_default_filename]

        # get root level datasets config if exist
        allowed_datasets_filename = ['datasets.' + sfx for sfx in ALLOWED_CONFIG_FILE_SUFFIX]
        maybe_datasets_file = [sub for sub in subs if sub in allowed_datasets_filename]

        # get the configurations that are not default
        subs_fpath = [os.path.join(config_dir, sub) for sub in subs if (sub not in maybe_default_file) and (sub not in maybe_datasets_file)]

        # if there is no default.yaml in current sub folder, use the defaults from the parent folder
        # if there is, merge the defaults
        default_config = parent_default_config or {}

        if len(maybe_default_file) > 0:
            default_file = maybe_default_file[0]
            default_fpath = os.path.join(config_dir, default_file)
            default_config = merge_configs(
                cls._load_config(
                    config_filepath=default_fpath
                ),
                default_config,
                keep_default_values=["dataset_config_file"]
            )

        # set root datasets file in config
        if root_level and len(maybe_datasets_file) > 0:
            datasets_file = maybe_datasets_file[0]
            datasets_file = os.path.join(config_dir, datasets_file)
            default_config["dataset_config_file"] = datasets_file
            
        # load dags from each yaml configuration files
        dag_import_failures = {}
        general_import_failures = {}
        for sub_fpath in subs_fpath:
            if os.path.isdir(sub_fpath):
                dag_failures, general_failures = cls._from_directory(sub_fpath, globals, default_config, root_level=False, config_filter=config_filter)
                dag_import_failures = dag_import_failures | dag_failures
                general_import_failures = general_import_failures | general_failures
            elif os.path.isfile(sub_fpath) and sub_fpath.split('.')[-1] in ALLOWED_CONFIG_FILE_SUFFIX:
                if config_filter and sub_fpath not in config_filter:
                    logger.info(f"Skipping {sub_fpath} due to filter")
                    continue

                if 'git/repo/dags/' in sub_fpath:
                    if sub_fpath.split("/")[-1].startswith("_jc__"):
                        owner = sub_fpath.split("/")[4]
                        default_config['default_args']['owner'] = owner
                        if owner == 'data_engineering':
                            default_config['tags'] = sub_fpath.split("/")[5:7]
                    else:
                        logger.info(f"Ignored invalid dag config file: {sub_fpath} ")
                        continue
                # catch the errors so the rest of the dags can still be imported
                try:
                    logger.info(f"Reading dag config file: {sub_fpath}")
                    logger.info(f"Generate dag: {default_config}")
                    dag_factory = cls(config_filepath=sub_fpath, default_config=default_config, enforce_global_datasets=True)
                    dag_factory.generate_dags(globals)
                except Exception as e:
                    if isinstance(e, DagFactoryConfigException):    
                        logger.info(f"Invalid dag config: {dag_import_failures}")
                        logger.info(f"Import error message: {str(e)}")
                        if cls.DAGBAG_IMPORT_ERROR_TRACEBACKS:
                            dag_import_failures[sub_fpath] = (traceback.format_exc(
                                limit=-cls.DAGBAG_IMPORT_ERROR_TRACEBACK_DEPTH)
                            , e.dag_id, e.tags)
                        else:
                            dag_import_failures[sub_fpath] = (str(e), e.dag_id, e.tags)
                    else:
                        logger.info(f"Import error message: {str(e)}")
                        if cls.DAGBAG_IMPORT_ERROR_TRACEBACKS:
                            general_import_failures[sub_fpath] = traceback.format_exc(
                                limit=-cls.DAGBAG_IMPORT_ERROR_TRACEBACK_DEPTH)
                        else:
                            general_import_failures[sub_fpath] = str(e)

        return dag_import_failures, general_import_failures


    @classmethod
    def from_directory(
        cls, 
        config_dir, 
        globals: Dict[str, Any], 
        config_filter: Optional[Set[str]]=None,
        raise_import_errors: Optional[bool]=False
        ):
        """
        Make instances of DagFactory for each yaml configuration files within a directory
        """
        dag_import_failures, general_import_failures = cls._from_directory(config_dir=config_dir, 
                                                                           globals=globals, 
                                                                           root_level=True, 
                                                                           config_filter=config_filter
                                                                           )

        # in the end we want to surface the error messages if there's any
        if dag_import_failures or general_import_failures:
            # reformat import_failures so they are reader friendly
            import_failures_reformatted = 'general import errors:\n'
            for import_loc, import_trc in general_import_failures.items():
                import_failures_reformatted += '\n' + f'Failure when generating dag from {import_loc}\n' + \
                                               '-'*100 + '\n' + import_trc + '\n'
            import_failures_reformatted += 'dag import errors:\n'
            for import_loc, import_info in dag_import_failures.items():
                import_trc = import_info[0]
                dag_id = import_info[1]
                import_failures_reformatted += '\n' + f'Failed to generate dag {dag_id} from {import_loc}\n' + \
                                               '-'*100 + '\n' + import_trc + '\n'
                
            if raise_import_errors:
                raise DagFactoryConfigException(import_failures_reformatted)
            
            alert_dag_id = (os.path.split(os.path.abspath(globals['__file__']))[-1]).split('.')[0] + \
                           '_dag_factory_import_error_messenger'
            
            dag_failure_map = {dag_id: 
                
                    {
                        "config_location": loc, 
                        "error_message": trace, 
                        "dag_id": dag_id, 
                        "tags": tags
                        }
                     for loc, (trace, dag_id, tags) in dag_import_failures.items() }
                
            with DAG(
                dag_id=alert_dag_id,
                schedule_interval="@once",
                default_args={
                    "depends_on_past": False,
                    "start_date": datetime.datetime(
                        2020, 1, 1, tzinfo=pendulum.timezone("America/New_York")
                    )
                },
                tags=[f"dag_factory_import_errors"]
            ) as alert_dag:
                full_errors = DummyOperator(
                    task_id='import_error_messenger',
                    doc_json=import_failures_reformatted
                )
                for dag_id, msg in dag_failure_map.items():
                    error_message = msg["error_message"]
                    del msg["error_message"]
                    DummyOperator(
                        dag=alert_dag,
                        task_id=f'import_error_messenger_{dag_id}',
                        doc_json=json.dumps(msg),
                        doc=error_message
                    )
                globals[alert_dag_id] = alert_dag

    @staticmethod
    def _serialise_config_md(dag_name, dag_config, default_config):
        # Remove empty task_groups if it exists
        # We inject it if not supply by user
        # https://github.com/astronomer/dag-factory/blob/e53b456d25917b746d28eecd1e896595ae0ee62b/dagfactory/dagfactory.py#L102
        if dag_config.get("task_groups") == {}:
            del dag_config["task_groups"]

        if any(tag in ('etl', 'reverse_etl') for tag in default_config.get("tags", [])) and not any(
                    tag.startswith("criticality:tier") for tag in dag_config.get("tags",[])):
                default_config["tags"].append("criticality:tier2")

        # Convert default_config to YAML format
        default_config = {"default": default_config}
        default_config_yaml = yaml.dump(default_config, default_flow_style=False, allow_unicode=True, sort_keys=False)

        # Convert dag_config to YAML format
        dag_config = {dag_name: dag_config}
        dag_config_yaml = yaml.dump(dag_config, default_flow_style=False, allow_unicode=True, sort_keys=False)

        # Combine the two YAML outputs with appropriate formatting
        dag_yml = default_config_yaml + "\n" + dag_config_yaml

        return dag_yml

    @staticmethod
    def _validate_config_filepath(config_filepath: str) -> None:
        """
        Validates config file path is absolute
        """
        if not os.path.isabs(config_filepath):
            raise DagFactoryConfigException("DAG Factory `config_filepath` must be absolute path")

    @staticmethod
    def _load_config(config_filepath: str) -> Dict[str, Any]:
        """
        Loads YAML config file to dictionary

        :returns: dict from YAML config file
        """
        # pylint: disable=consider-using-with
        try:

            def __join(loader: yaml.FullLoader, node: yaml.Node) -> str:
                seq = loader.construct_sequence(node)
                return "".join([str(i) for i in seq])

            def __or(loader: yaml.FullLoader, node: yaml.Node) -> str:
                seq = loader.construct_sequence(node)
                return " | ".join([f"({str(i)})" for i in seq])

            def __and(loader: yaml.FullLoader, node: yaml.Node) -> str:
                seq = loader.construct_sequence(node)
                return " & ".join([f"({str(i)})" for i in seq])

            yaml.add_constructor("!join", __join, yaml.FullLoader)
            yaml.add_constructor("!or", __or, yaml.FullLoader)
            yaml.add_constructor("!and", __and, yaml.FullLoader)

            with open(config_filepath, "r", encoding="utf-8") as fp:
                config_with_env = os.path.expandvars(fp.read())
                config: Dict[str, Any] = yaml.load(stream=config_with_env, Loader=yaml.FullLoader)
        except Exception as err:
            raise DagFactoryConfigException("Invalid DAG Factory config file") from err
        return config

    def get_dag_configs(self) -> Dict[str, Dict[str, Any]]:
        """
        Returns configuration for each the DAG in factory

        :returns: dict with configuration for dags
        """
        return {dag: self.config[dag] for dag in self.config.keys() if dag not in SYSTEM_PARAMS}

    def get_default_config(self) -> Dict[str, Any]:
        """
        Returns defaults for the DAG factory. If no defaults exist, returns empty dict.

        :returns: dict with default configuration
        """
        if self.default_config:
            return self.default_config
        return self.config.get("default", {})

    def build_dags(self) -> Dict[str, DAG]:
        """Build DAGs using the config file."""
        dag_configs: Dict[str, Dict[str, Any]] = self.get_dag_configs()
        default_config: Dict[str, Any] = self.get_default_config()

        dags: Dict[str, Any] = {}

        for dag_name, dag_config in dag_configs.items():
            dag_config["task_groups"] = dag_config.get("task_groups", {})
            dag_builder: DagBuilder = DagBuilder(
                dag_name=dag_name,
                dag_config=dag_config,
                default_config=default_config,
                yml_dag=self._serialise_config_md(dag_name, dag_config, default_config),
                enforce_global_datasets=self.enforce_global_datasets
            )
            try:
                dag: Dict[str, Union[str, DAG]] = dag_builder.build()
                if isinstance(dag["dag"], DAG):
                    dags[dag["dag_id"]]: DAG = dag["dag"]
                elif isinstance(dag["dag"], str):
                    logger.info(f"dag {dag['dag_id']} was not imported. reason: {dag['dag']}")
            except Exception as err:
                tags = dag_config.get("tags", [])
                raise DagFactoryConfigException(f"Failed to generate dag {dag_name}. verify config is correct", dag_id=dag_name, tags=tags) from err

        return dags

    # pylint: disable=redefined-builtin
    @staticmethod
    def register_dags(dags: Dict[str, DAG], globals: Dict[str, Any]) -> None:
        """Adds `dags` to `globals` so Airflow can discover them.

        :param: dags: Dict of DAGs to be registered.
        :param globals: The globals() from the file used to generate DAGs. The dag_id
            must be passed into globals() for Airflow to import
        """
        for dag_id, dag in dags.items():
            globals[dag_id]: DAG = dag

    def generate_dags(self, globals: Dict[str, Any]) -> None:
        """
        Generates DAGs from YAML config

        :param globals: The globals() from the file used to generate DAGs. The dag_id
            must be passed into globals() for Airflow to import
        """
        dags: Dict[str, Any] = self.build_dags()
        self.register_dags(dags, globals)

    def clean_dags(self, globals: Dict[str, Any]) -> None:
        """
        Clean old DAGs that are not on YAML config but were auto-generated through dag-factory

        :param globals: The globals() from the file used to generate DAGs. The dag_id
            must be passed into globals() for Airflow to import
        """
        dags: Dict[str, Any] = self.build_dags()

        # filter dags that exists in globals and is auto-generated by dag-factory
        dags_in_globals: Dict[str, Any] = {}
        for k, glb in globals.items():
            if isinstance(glb, DAG) and hasattr(glb, "is_dagfactory_auto_generated"):
                dags_in_globals[k] = glb

        # finding dags that doesn't exist anymore
        dags_to_remove: List[str] = list(set(dags_in_globals) - set(dags))

        # removing dags from DagBag
        for dag_to_remove in dags_to_remove:
            del globals[dag_to_remove]

