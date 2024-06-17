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
    schedule_interval='@daily',
    catchup=False
)


def generate_suppliers_data():
    suppliers = []
    for _ in range(random.randint(1, 5)):
        rand_row = [fake.company(), None, None, None, None, None, None, _generate_nullable_country(), None, None, None]
        suppliers.append(rand_row)
    return suppliers


# def generate_orders_data():
#     orders = []
#     for _ in range(random.randint(1, 5)):
#         row = [None, None, fake.date() if random.random() < 0.5 else None, None, None, None, None, None, None, None, None, None, None]
#         orders.append(row)
#     return orders
#
#
# def generate_products_data():
#     products = []
#     for _ in range(random.randint(1, 5)):
#         row = []


def _generate_nullable_country():
    if random.random() < 0.2:
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

# orders_data = generate_orders_data()
#
# orders_values_sql = ', '.join(['(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'] * len(orders_data))
#
# insert_orders = PostgresOperator(
#     task_id="insert_orders",
#     postgres_conn_id="postgres_source",
#     sql=f"""
#     INSERT INTO orders (customer_id, employee_id, order_date, required_date, shipped_date, ship_via, freight, ship_name, ship_address, ship_city, ship_region, ship_postal_code, ship_country)
#     VALUES {orders_values_sql}
#     """,
#     parameters=[item for sublist in orders_data for item in sublist],
#     dag=dag
# )
