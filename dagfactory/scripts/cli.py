import os
from pathlib import Path

import click

from dagfactory.cicd.validate_dag_imports import validate_imports
from dagfactory.cicd.validate_datasets import validate_datasets_file

@click.group()
@click.option('--logdir', type=click.Path(file_okay=False, path_type=Path), default=None, help='Directory to write logs to. Default: path/to/dbt-repo/logs')
@click.pass_context
def cli(ctx, logdir):
    """Tools for DagFactory dbt projects
    """
    ctx.ensure_object(dict)
    ctx.obj['logdir'] = (logdir)


@cli.command()
@click.option('--config-dir', '-d', type=click.Path(exists=True, file_okay=False), help='Directory with DAG configs to parse')
@click.option('--filter', '-f', multiple=True, type=click.Path(exists=True, dir_okay=False), help='DAG config filepaths to include. Must point to a file')
@click.option('--filter-file', type=click.Path(exists=True, dir_okay=False), help='File containing a list of config files to include ')
def validate_yaml_dags(config_dir, filter, filter_file):
    """Generate yaml dags to validate configurations

    Superset of all .yaml or .yml job config files in the config-dir (recursive)
    will be parsed. 
    if no filter option is passed all configs in the directory will be loaded.
    To filter which configs are parsed include --filter filepath
    filepath must be absolute. --filter can be passed multiple times to include multiple files.
    Alternativly use --filter-file filepath to pass a text file with a list of paths to include.

    --filter and --filter-file can be used together. The union of the 2 options will be used to create the filter
    """
    filter_paths = []
    if filter_file:
        try:
            with open(filter_file) as file:
                filter_paths = [path.rstrip() for path in file]
        except Exception as e:
            print(f'invalid filter file: {e}')
            raise Exception(e)
    
    filter_paths += filter
    validate_imports(target=config_dir, filter = set(filter_paths))


@cli.command()
@click.argument('config_fp', type=click.Path(exists=True, dir_okay=False))
def validate_datasets_config(config_fp):
    """Validate datasets.yml file
        dagfactory validate-datasets-file filepath
        filepath must be absolute
    """
    validate_datasets_file(config_fp)


if __name__ == '__main__':
    cli(obj={})
