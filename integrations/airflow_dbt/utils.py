from dbt.main import parse_args
from argparse import Namespace


def parse_command(command: str) -> Namespace:
    command = command.strip()
    assert command.startswith("dbt"), "dbt command should start with key word 'dbt'"
    args = command.split()[1:]  # remove 'dbt' from args
    parsed = parse_args(args)
    return parsed


