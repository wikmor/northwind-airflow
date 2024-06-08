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
    'transfer_dag',
    default_args=default_args,
    description='Transfer data between PostgreSQL databases using Python hooks',
    schedule_interval='@daily',
    catchup=False
)


def transfer_data():
    source_hook = PostgresHook(postgres_conn_id='postgres_source')
    star_hook = PostgresHook(postgres_conn_id='postgres_star')

    # SQL to fetch data from source database
    source_sql = "SELECT customer_id, company_name, city, country FROM customers;"
    source_data = source_hook.get_records(source_sql)

    destination_sql = "INSERT INTO customer (customer_id, company_name, city, country) VALUES (%s, %s, %s, %s)"
    # SQL to insert data into destination database
    if source_data:
        for row in source_data:
            star_hook.run(destination_sql, parameters=row)

    source_sql = """SELECT DISTINCT 
        order_id AS time_id, 
        order_date, 
        date_part('year', order_date) AS year, 
        date_part('quarter', order_date) AS quarter, 
        date_part('month', order_date) AS month, 
        date_part('day', order_date) AS day 
    FROM orders;"""
    source_data = source_hook.get_records(source_sql)

    destination_sql = "INSERT INTO time (time_id, date, year, quarter, month, day) VALUES (%s, %s, %s, %s, %s, %s)"
    # SQL to insert data into destination database
    if source_data:
        for row in source_data:
            star_hook.run(destination_sql, parameters=row)


transfer_task = PythonOperator(
    task_id='transfer_data',
    python_callable=transfer_data,
    dag=dag
)
