import yaml
import re
import sys
from yaml import SafeLoader
VALID_KEYS = ["name", "uri", "__line__"]

class DatasetConfigException(Exception):
    """
    Exception to raise when invalid dataset configurations are present
    """

class SafeLineLoader(SafeLoader):
    # Load in yaml with line numbers to allow for easier debugging
    def construct_mapping(self, node, deep=False):
        mapping = super(SafeLineLoader, self).construct_mapping(node, deep=deep)
        # Add 1 so line numbering starts at 1
        mapping["__line__"] = node.start_mark.line + 1
        return mapping

def load_config(dataset_file):
    with open(dataset_file) as file:
        return yaml.load(file, Loader=SafeLineLoader)

def validate_config(config, valid_keys):
    if "datasets" not in config:
        return ["datasets"], list(config.keys())
    invalid_or_missing = {}
    valid_set = set(valid_keys)
    for dataset in config["datasets"]:
        key_set = set(dataset.keys())
        line = dataset["__line__"]
        if key_set != valid_set:
            invalid_or_missing[line] = {}
            invalid_or_missing[line]["invalid"] = key_set.difference(valid_set)
            invalid_or_missing[line]["missing"] = valid_set.difference(key_set)
    return invalid_or_missing
    
def validate_datasets(config):
    duplicate_names = {}
    duplicate_uris = {}
    invalid_uris = []
    for dataset in config["datasets"]:
        line = dataset["__line__"]
        name = dataset["name"]
        uri = dataset["uri"]
        uri_error = validate_uri(uri, line)
        if uri_error:
            invalid_uris.append(uri_error)
        if name in duplicate_names:
            duplicate_names[name].append(line)
        else:
            duplicate_names[name] = [line]
        
        if uri in duplicate_uris:
            duplicate_uris[uri].append(line)
        else:
            duplicate_uris[uri] = [line]

    return {k:v for k,v in duplicate_names.items() if len(v) > 1}, {k:v for k,v in duplicate_uris.items() if len(v) > 1}, invalid_uris
    
def validate_uri(uri, line):    
    pattern = r"(?P<type>\w+)://.*"
    redshift_pattern = r"^redshift://([^\.\s\t]+\.){3}[^\.\s\t]+$"
    match = re.match(pattern, uri)
    if not match:
        return f"invalid uri: {uri} in dataset starting on line: {line}. URI must start with <type>://"
    if match.group("type") == "redshift":
        return "" if re.match(redshift_pattern, uri) else f"invalid uri: {uri} in dataset starting on line: {line}. Redshift URI must be of format redshift://cluster.db.schema.table"


def validate_datasets_file(filename):
    config = load_config(filename)
    invalid_or_missing = validate_config(config, VALID_KEYS)
    error_message = "Invalid Dataset Config:\n"
    for line, errors in invalid_or_missing.items():
        error_message += f"Errors in dataset config starting on line {line}:\n"
        if errors["missing"]:
            error_message += f"\tRequired keys missing from config: {errors['missing']}\n"
        if errors["invalid"]:
            error_message += f"\tInvalid keys in config from config: {errors['invalid']}\n"
    if invalid_or_missing:
        raise DatasetConfigException(error_message)
    
    duplicate_names, duplicate_uris, invalid_uris = validate_datasets(config)

    if duplicate_names:
        error_message += "Duplicate dataset names in config:\n"
        for name, lines in duplicate_names.items():
            error_message += f"\tname: {name}\n\tlines: {', '.join(lines)}\n"
    if duplicate_uris:
        error_message += "\nDuplicate dataset URIs in config:\n"
        for uri, lines in duplicate_uris.items():
            error_message += f"\tname: {uri}\n\tlines: {', '.join(lines)}\n"
    if invalid_uris:
        error_message += "\nInvalid dataset URIs in config:\n\t"
        error_message += "\n\t".join(invalid_uris)
    if duplicate_names or duplicate_uris or invalid_uris:
        raise DatasetConfigException(error_message)
