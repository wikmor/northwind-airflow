from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'transform_source_to_staging_dag',
    default_args=default_args,
    description='Clean and transfer data between source and staging databases',
    schedule_interval='@daily',
    catchup=False
)


def remove_and_create_suppliers():
    PostgresHook(postgres_conn_id='postgres_staging').run("TRUNCATE TABLE suppliers_tmp;")  # TODO Externalize PostgresHook


def clean_suppliers():
    source_sql = "SELECT supplier_id, company_name, COALESCE(country, 'NoCountryProvided') FROM suppliers;"

    destination_sql = "INSERT INTO suppliers_tmp (supplier_id, company_name, country) VALUES (%s, %s, %s)"

    _transfer_records(source_sql, destination_sql)


def _transfer_records(source_sql, destination_sql):
    source_hook = PostgresHook(postgres_conn_id='postgres_source')
    staging_hook = PostgresHook(postgres_conn_id='postgres_staging')

    source_data = source_hook.get_records(source_sql)
    if source_data:
        for row in source_data:
            staging_hook.run(destination_sql, parameters=row)


remove_suppliers_table = PythonOperator(
    task_id='remove_and_create_suppliers_table',
    python_callable=remove_and_create_suppliers,
    dag=dag
)

supplier_task = PythonOperator(
    task_id='clean_suppliers',
    python_callable=clean_suppliers,
    dag=dag
)

remove_suppliers_table >> supplier_task
