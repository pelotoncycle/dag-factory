# MANIFEST_PATH = '/Users/terry.yin/work/repos/dagster-demo/tutorial_dbt_dagster/tutorial_finished/jaffle_shop/target/manifest.json'
# MANIFEST_PATH = '/dbt-airflow/ds-dbt/manifest.json'
MANIFEST_PATH = '/Users/terry.yin/work/repos/ds-dbt/target/manifest.json'
PROJECT_PATH = '/Users/terry.yin/work/repos/ds-dbt/'
# PROJECT_PATH = '/Users/terry.yin/work/repos/dagster-demo/tutorial_dbt_dagster/tutorial_finished/jaffle_shop/'

DBT_OPERATOR_IMPORT_PATHS = {
    'DbtCLIOperator': 'operators.dbt_operators.dbt_cli_operator.DbtCLIOperator',
    'DbtCLISensorOperator': 'operators.dbt_operators.dbt_cli_operator.DbtCLISensorOperator',
    'DbtTestOperator': 'operators.dbt_operators.dbt_cli_operator.DbtTestOperator'
}
