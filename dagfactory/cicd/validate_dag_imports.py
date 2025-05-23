from dagfactory.dagfactory import DagFactory

def validate_imports(target, filter):
    DagFactory.from_directory(config_dir=target, globals=globals(), config_filter=filter, raise_import_errors=True, validate_dags_after_load=True)
