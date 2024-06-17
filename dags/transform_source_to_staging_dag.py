from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
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
    'transform_source_to_staging_dag',
    default_args=default_args,
    description='Clean and transfer data between source and staging databases',
    schedule_interval='@daily',
    catchup=False
)

source = PostgresHook(postgres_conn_id='postgres_source')
staging = PostgresHook(postgres_conn_id='postgres_staging')
star = PostgresHook(postgres_conn_id='postgres_star')


# class Staging:
#     def __init__(self):
#         self.staging = PostgresHook(postgres_conn_id='postgres_staging')
#
#     @staticmethod
#     def query_multiple(data, sql):
#         for record in data:
#             staging.run(sql, parameters=record)


def truncate_tables():
    staging.run("TRUNCATE TABLE suppliers_tmp, orders_tmp;")


def clean_suppliers():
    suppliers = fetch_suppliers()

    cleaned_source_data = [
        (supplier_id, fake.company(), country if country is not None else 'NoCountryProvided')
        for supplier_id, company_name, country in suppliers
    ]

    sql = "INSERT INTO suppliers_tmp (supplier_id, company_name, country) VALUES (%s, %s, %s)"
    staging_query_multiple(cleaned_source_data, sql)


def staging_query_multiple(data, sql):
    for record in data:
        staging.run(sql, parameters=record)


def fetch_suppliers():
    return source.get_records("SELECT supplier_id, company_name, country FROM suppliers;")


def clean_orders():
    orders = fetch_orders()

    cleaned_data = [
        (order_id, order_date) for order_id, order_date in orders
    ]

    destination_sql = "INSERT INTO orders_tmp (order_id, order_date) VALUES (%s, %s)"

    for record in cleaned_data:
        staging.run(destination_sql, parameters=record)


def fetch_orders():
    return source.get_records("SELECT order_id, order_date FROM orders;")


def clean_employees():
    employees = fetch_employees()
    cleaned_data = [
        (employee_id, last_name, first_name, reports_to) for employee_id, last_name, first_name, reports_to in employees
    ]

    destination_sql = "INSERT INTO employees_tmp (employee_id, last_name, first_name, reports_to) VALUES (%s, %s, %s, %s)"

    staging_query_multiple(cleaned_data, destination_sql)


def fetch_employees():
    return source.get_records("SELECT employee_id, last_name, first_name, reports_to FROM employees;")


def clean_customers():
    customers = fetch_customers()
    cleaned_data = [
        (customer_id, company_name, city, country) for customer_id, company_name, city, country in customers
    ]

    destination_sql = "INSERT INTO customers_tmp (customer_id, company_name, city, country) VALUES (%s, %s, %s, %s)"

    staging_query_multiple(cleaned_data, destination_sql)


def fetch_customers():
    return source.get_records("SELECT customer_id, company_name, city, country FROM customers")


def clean_products():
    products = fetch_products()
    cleaned_data = [
        (product_id, product_name, supplier_id, category_id) for product_id, product_name, supplier_id, category_id in
        products
    ]

    destination_sql = "INSERT INTO products_tmp (product_id, product_name, supplier_id, category_id) VALUES (%s, %s, %s, %s)"

    staging_query_multiple(cleaned_data, destination_sql)


def fetch_products():
    return source.get_records("SELECT product_id, product_name, supplier_id, category_id FROM products")


def clean_categories():
    categories = fetch_categories()
    cleaned_data = [
        (category_id, category_name) for category_id, category_name in categories
    ]

    destination_sql = "INSERT INTO categories_tmp (category_id, category_name) VALUES (%s, %s)"

    staging_query_multiple(cleaned_data, destination_sql)


def fetch_categories():
    return source.get_records("SELECT category_id, category_name FROM categories")


def clean_order_details():
    order_details = fetch_order_details()
    cleaned_data = [
        (order_id, product_id, unit_price, quantity) for order_id, product_id, unit_price, quantity in order_details
    ]

    destination_sql = "INSERT INTO order_details_tmp (order_id, product_id, unit_price, quantity) VALUES (%s, %s, %s, %s)"

    staging_query_multiple(cleaned_data, destination_sql)


def fetch_order_details():
    return source.get_records("SELECT order_id, product_id, unit_price, quantity FROM order_details")


truncate_tables_task = PythonOperator(
    task_id='truncate_tables',
    python_callable=truncate_tables,
    dag=dag
)

fetch_suppliers_task = PythonOperator(
    task_id='fetch_suppliers',
    python_callable=fetch_suppliers,
    dag=dag
)

supplier_task = PythonOperator(
    task_id='clean_suppliers',
    python_callable=clean_suppliers,
    dag=dag
)
#
fetch_orders_task = PythonOperator(
    task_id='fetch_orders',
    python_callable=fetch_orders,
    dag=dag
)

orders_task = PythonOperator(
    task_id='clean_orders',
    python_callable=clean_orders,
    dag=dag
)

fetch_employees_task = PythonOperator(
    task_id='fetch_employees',
    python_callable=fetch_employees,
    dag=dag
)

employees_task = PythonOperator(
    task_id='clean_employees',
    python_callable=clean_employees,
    dag=dag
)

fetch_customers_task = PythonOperator(
    task_id='fetch_customers',
    python_callable=fetch_customers,
    dag=dag
)

customers_task = PythonOperator(
    task_id='clean_customers',
    python_callable=clean_customers,
    dag=dag
)

fetch_products_task = PythonOperator(
    task_id='fetch_products',
    python_callable=fetch_products,
    dag=dag
)

products_task = PythonOperator(
    task_id='clean_products',
    python_callable=clean_customers,
    dag=dag
)

fetch_categories_task = PythonOperator(
    task_id='fetch_categories',
    python_callable=fetch_categories,
    dag=dag
)

categories_task = PythonOperator(
    task_id='clean_categories',
    python_callable=clean_categories,
    dag=dag
)

fetch_order_details_task = PythonOperator(
    task_id='fetch_order_details',
    python_callable=fetch_order_details,
    dag=dag
)

order_details_task = PythonOperator(
    task_id='clean_order_details',
    python_callable=clean_order_details,
    dag=dag
)

[supplier_task << fetch_suppliers_task,
 orders_task << fetch_orders_task,
 employees_task << fetch_employees_task,
 customers_task << fetch_customers_task,
 products_task << fetch_products_task,
 categories_task << fetch_categories_task,
 order_details_task << fetch_order_details_task] << truncate_tables_task
