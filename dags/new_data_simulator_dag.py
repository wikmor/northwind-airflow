import random

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

from faker import Faker

fake = Faker()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'new_data_simulator_dag',
    default_args=default_args,
    description='Generate new data (with nulls) and add it to source database',
    schedule_interval='@daily',
    catchup=False
)

data = [
    (fake.company(), None, None, None, None, None, None, None, None, None, None),
    (fake.company(), None, None, None, None, None, None, fake.country(), None, None, None),
]

insert_rows = PostgresOperator(
    task_id="insert_rows",
    postgres_conn_id="postgres_source",
    sql=f"""
    INSERT INTO suppliers (company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax, homepage) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s), (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """,
    parameters=[item for sublist in data for item in sublist],
    dag=dag
)


# def generate_new_data():
#     source_hook = PostgresHook(postgres_conn_id='postgres_source')
#     company_name1, country1 = generate_suppliers()
#     insert_sql = """
#     INSERT INTO suppliers VALUES (company_name1, NULL, NULL, NULL, NULL, NULL, NULL, country1, NULL, NULL, NULL);
#     """
#     source_hook.run(insert_sql)


# def generate_suppliers():
#     company_name = fake.company()
#     country = _generate_nullable_country()
#     return company_name, country
#
#
# def _generate_nullable_country():
#     if random.random() < 0.2:
#         return None
#     else:
#         return fake.country()
