import random

from airflow import DAG
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
    schedule_interval=None,
    catchup=False
)


def generate_suppliers_data():
    suppliers = []
    for _ in range(random.randint(1, 5)):
        rand_row = [
            fake.company(),
            fake.name(),
            fake.job(),
            fake.address(),
            fake.city(),
            fake.city(),
            fake.postalcode(),
            _generate_nullable_country(),
            fake.phone_number(),
            fake.phone_number(),
            fake.url()
        ]
        suppliers.append(rand_row)
    return suppliers


def _generate_nullable_country():
    if random.random() < 0.75:
        return None
    else:
        return fake.country()


suppliers_data = generate_suppliers_data()

suppliers_values_sql = ', '.join(['(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'] * len(suppliers_data))

insert_suppliers = PostgresOperator(
    task_id="insert_suppliers",
    postgres_conn_id="postgres_source",
    sql=f"""
    INSERT INTO suppliers (company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax, homepage) 
    VALUES {suppliers_values_sql}
    """,
    parameters=[item for sublist in suppliers_data for item in sublist],
    dag=dag
)
