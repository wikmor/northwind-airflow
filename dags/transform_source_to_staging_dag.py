from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from faker import Faker
from utils.staging import StagingUtil
from collections.abc import Callable
from typing import Any

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


class Transform:
    def __init__(
            self,
            entity: str,
            fetch_sql: str,
            clean_func,
            insert_sql: str
    ) -> None:
        self.entity = entity
        self.fetch_sql = fetch_sql
        self.clean_func = clean_func
        self.insert_sql = insert_sql

    def transfer_data(self):
        data = self.fetch_data()
        cleaned_data = self.clean_data(data)
        StagingUtil.query_multiple(self.insert_sql, cleaned_data)

    def fetch_data(self):
        return source.get_records(self.fetch_sql)

    def clean_data(self, data):
        return self.clean_func(data)


def truncate_tables():
    staging.run("""
    TRUNCATE TABLE order_details_tmp, products_tmp, categories_tmp, customers_tmp, employees_tmp, suppliers_tmp, orders_tmp;
    """)


def clean_suppliers(data):
    return [
        (supplier_id, fake.company(), country if country is not None else 'NoCountryProvided')
        for supplier_id, company_name, country in data
    ]


def clean_orders(data):
    return [(order_id, order_date) for order_id, order_date in data]


def clean_employees(data):
    return [
        (employee_id, last_name, first_name, reports_to) for employee_id, last_name, first_name, reports_to in data
    ]


def clean_customers(data):
    return [
        (customer_id, company_name, city, country) for customer_id, company_name, city, country in data
    ]


def clean_products(data):
    return [
        (product_id, product_name, supplier_id, category_id) for product_id, product_name, supplier_id, category_id in
        data
    ]


def clean_categories(data):
    return [(category_id, category_name) for category_id, category_name in data]


def clean_order_details(data):
    return [
        (order_id, product_id, unit_price, quantity) for order_id, product_id, unit_price, quantity in data
    ]


entities = {
    'suppliers': Transform(
        entity='suppliers',
        fetch_sql="SELECT supplier_id, company_name, country FROM suppliers;",
        insert_sql="INSERT INTO suppliers_tmp (supplier_id, company_name, country) VALUES (%s, %s, %s)",
        clean_func=clean_suppliers
    ),
    'orders': Transform(
        entity='orders',
        fetch_sql="SELECT order_id, order_date FROM orders;",
        insert_sql="INSERT INTO orders_tmp (order_id, order_date) VALUES (%s, %s)",
        clean_func=clean_orders
    ),
    'employees': Transform(
        entity='employees',
        fetch_sql="SELECT employee_id, last_name, first_name, reports_to FROM employees;",
        insert_sql="INSERT INTO employees_tmp (employee_id, last_name, first_name, reports_to) VALUES (%s, %s, %s, %s)",
        clean_func=clean_employees
    ),
    'customers': Transform(
        entity='customers',
        fetch_sql="SELECT customer_id, company_name, city, country FROM customers;",
        insert_sql="INSERT INTO customers_tmp (customer_id, company_name, city, country) VALUES (%s, %s, %s, %s)",
        clean_func=clean_customers
    ),
    'products': Transform(
        entity='products',
        fetch_sql="SELECT product_id, product_name, supplier_id, category_id FROM products;",
        insert_sql="INSERT INTO products_tmp (product_id, product_name, supplier_id, category_id) VALUES (%s, %s, %s, %s)",
        clean_func=clean_products
    ),
    'categories': Transform(
        entity='categories',
        fetch_sql="SELECT category_id, category_name FROM categories;",
        insert_sql="INSERT INTO categories_tmp (category_id, category_name) VALUES (%s, %s)",
        clean_func=clean_categories
    ),
    'order_details': Transform(
        entity='order_details',
        fetch_sql="SELECT order_id, product_id, unit_price, quantity FROM order_details;",
        insert_sql="INSERT INTO order_details_tmp (order_id, product_id, unit_price, quantity) VALUES (%s, %s, %s, %s)",
        clean_func=clean_order_details
    ),
}

truncate_tables_task = PythonOperator(
    task_id='truncate_tables',
    python_callable=truncate_tables,
    dag=dag
)

fetch_tasks = []
clean_tasks = []
transfer_tasks = []

for entity_name, entity in entities.items():
    fetch_task = PythonOperator(
        task_id=f'fetch_{entity_name}',
        python_callable=entity.fetch_data,
        dag=dag
    )

    clean_task = PythonOperator(
        task_id=f'clean_{entity_name}',
        python_callable=lambda e=entity: e.clean_data(e.fetch_data()),
        dag=dag
    )

    transfer_task = PythonOperator(
        task_id=f'transfer_{entity_name}',
        python_callable=entity.transfer_data,
        dag=dag
    )

    fetch_task >> clean_task >> transfer_task

    fetch_tasks.append(fetch_task)
    clean_tasks.append(clean_task)
    transfer_tasks.append(transfer_task)

order_details_transfer_task = next(task for task in transfer_tasks if task.task_id == 'transfer_order_details')
products_transfer_task = next(task for task in transfer_tasks if task.task_id == 'transfer_products')
orders_transfer_task = next(task for task in transfer_tasks if task.task_id == 'transfer_orders')
categories_transfer_task = next(task for task in transfer_tasks if task.task_id == 'transfer_categories')
suppliers_transfer_task = next(task for task in transfer_tasks if task.task_id == 'transfer_suppliers')

# Ensure order_details_transfer_task runs after products_transfer_task and orders_transfer_task
order_details_transfer_task.set_upstream(products_transfer_task)
order_details_transfer_task.set_upstream(orders_transfer_task)

# Ensure products_transfer_task runs after categories_transfer_task and suppliers_transfer_task
products_transfer_task.set_upstream(categories_transfer_task)
products_transfer_task.set_upstream(suppliers_transfer_task)

truncate_tables_task >> fetch_tasks


truncate_tables_task >> fetch_tasks

    # truncate_tables_task >> fetch_task >> clean_task >> transfer_task

# [task for entity in entities.values() for task in [transfer_task, clean_task, fetch_task]] << truncate_tables_task

#
#
# def transfer_suppliers():
#     cleaned_data = clean_suppliers()
#     sql = "INSERT INTO suppliers_tmp (supplier_id, company_name, country) VALUES (%s, %s, %s)"
#     StagingUtil.query_multiple(sql, cleaned_data)
#
#
# def clean_suppliers():
#     suppliers = fetch_suppliers()
#
#     cleaned_source_data = [
#         (supplier_id, fake.company(), country if country is not None else 'NoCountryProvided')
#         for supplier_id, company_name, country in suppliers
#     ]
#
#     return cleaned_source_data
#
#
# def fetch_suppliers():
#     return source.get_records("SELECT supplier_id, company_name, country FROM suppliers;")
#
#
# def transfer_orders():
#     cleaned_data = clean_orders()
#     sql = "INSERT INTO orders_tmp (order_id, order_date) VALUES (%s, %s)"
#     StagingUtil.query_multiple(sql, cleaned_data)
#
#
# def clean_orders():
#     orders = fetch_orders()
#
#     cleaned_data = [
#         (order_id, order_date) for order_id, order_date in orders
#     ]
#
#     return cleaned_data
#
#
# def fetch_orders():
#     return source.get_records("SELECT order_id, order_date FROM orders;")
#
#
# def transfer_employees():
#     cleaned_data = clean_employees()
#     sql = "INSERT INTO employees_tmp (employee_id, last_name, first_name, reports_to) VALUES (%s, %s, %s, %s)"
#     StagingUtil.query_multiple(sql, cleaned_data)
#
#
# def clean_employees():
#     employees = fetch_employees()
#     cleaned_data = [
#         (employee_id, last_name, first_name, reports_to) for employee_id, last_name, first_name, reports_to in employees
#     ]
#
#     return cleaned_data
#
#
# def fetch_employees():
#     return source.get_records("SELECT employee_id, last_name, first_name, reports_to FROM employees;")
#
#
# def transfer_customers():
#     cleaned_data = clean_employees()
#     sql = "INSERT INTO customers_tmp (customer_id, company_name, city, country) VALUES (%s, %s, %s, %s)"
#     StagingUtil.query_multiple(sql, cleaned_data)
#
#
# def clean_customers():
#     customers = fetch_customers()
#     cleaned_data = [
#         (customer_id, company_name, city, country) for customer_id, company_name, city, country in customers
#     ]
#
#     return cleaned_data
#
#
# def fetch_customers():
#     return source.get_records("SELECT customer_id, company_name, city, country FROM customers")
#
#
# def transfer_products():
#     cleaned_data = clean_employees()
#     sql = "INSERT INTO products_tmp (product_id, product_name, supplier_id, category_id) VALUES (%s, %s, %s, %s)"
#     StagingUtil.query_multiple(sql, cleaned_data)
#
#
# def clean_products():
#     products = fetch_products()
#     cleaned_data = [
#         (product_id, product_name, supplier_id, category_id) for product_id, product_name, supplier_id, category_id in
#         products
#     ]
#
#     return cleaned_data
#
#
# def fetch_products():
#     return source.get_records("SELECT product_id, product_name, supplier_id, category_id FROM products")
#
#
# def transfer_categories():
#     cleaned_data = clean_employees()
#     sql = "INSERT INTO categories_tmp (category_id, category_name) VALUES (%s, %s)"
#     StagingUtil.query_multiple(sql, cleaned_data)
#
#
# def clean_categories():
#     categories = fetch_categories()
#     cleaned_data = [
#         (category_id, category_name) for category_id, category_name in categories
#     ]
#
#     return cleaned_data
#
#
# def fetch_categories():
#     return source.get_records("SELECT category_id, category_name FROM categories")
#
#
# def transfer_order_details():
#     cleaned_data = clean_order_details()
#     sql = "INSERT INTO order_details_tmp (order_id, product_id, unit_price, quantity) VALUES (%s, %s, %s, %s)"
#     StagingUtil.query_multiple(sql, cleaned_data)
#
#
# def clean_order_details():
#     order_details = fetch_order_details()
#     cleaned_data = [
#         (order_id, product_id, unit_price, quantity) for order_id, product_id, unit_price, quantity in order_details
#     ]
#     return cleaned_data
#
#
# def fetch_order_details():
#     return source.get_records("SELECT order_id, product_id, unit_price, quantity FROM order_details")
#
#
# truncate_tables_task = PythonOperator(
#     task_id='truncate_tables',
#     python_callable=truncate_tables,
#     dag=dag
# )
#
# transfer_suppliers_task = PythonOperator(
#     task_id='transfer_suppliers',
#     python_callable=transfer_suppliers,
#     dag=dag
# )
#
# clean_suppliers_task = PythonOperator(
#     task_id='clean_suppliers',
#     python_callable=clean_suppliers,
#     dag=dag
# )
#
# fetch_suppliers_task = PythonOperator(
#     task_id='fetch_suppliers',
#     python_callable=fetch_suppliers,
#     dag=dag
# )
#
# transfer_orders_task = PythonOperator(
#     task_id='transfer_orders',
#     python_callable=transfer_orders,
#     dag=dag
# )
#
# clean_orders_task = PythonOperator(
#     task_id='clean_orders',
#     python_callable=clean_orders,
#     dag=dag
# )
#
# fetch_orders_task = PythonOperator(
#     task_id='fetch_orders',
#     python_callable=fetch_orders,
#     dag=dag
# )
#
# transfer_employees_task = PythonOperator(
#     task_id='transfer_employees',
#     python_callable=transfer_employees,
#     dag=dag
# )
#
# clean_employees_task = PythonOperator(
#     task_id='clean_employees',
#     python_callable=clean_employees,
#     dag=dag
# )
#
# fetch_employees_task = PythonOperator(
#     task_id='fetch_employees',
#     python_callable=fetch_employees,
#     dag=dag
# )
#
# transfer_customers_task = PythonOperator(
#     task_id='transfer_customers',
#     python_callable=transfer_customers,
#     dag=dag
# )
#
# clean_customers_task = PythonOperator(
#     task_id='clean_customers',
#     python_callable=clean_customers,
#     dag=dag
# )
#
# fetch_customers_task = PythonOperator(
#     task_id='fetch_customers',
#     python_callable=fetch_customers,
#     dag=dag
# )
#
# transfer_products_task = PythonOperator(
#     task_id='transfer_products',
#     python_callable=transfer_products,
#     dag=dag
# )
#
# clean_products_task = PythonOperator(
#     task_id='clean_products',
#     python_callable=clean_customers,
#     dag=dag
# )
#
# fetch_products_task = PythonOperator(
#     task_id='fetch_products',
#     python_callable=fetch_products,
#     dag=dag
# )
#
# transfer_categories_task = PythonOperator(
#     task_id='transfer_categories',
#     python_callable=transfer_categories,
#     dag=dag
# )
#
# clean_categories_task = PythonOperator(
#     task_id='clean_categories',
#     python_callable=clean_categories,
#     dag=dag
# )
#
# fetch_categories_task = PythonOperator(
#     task_id='fetch_categories',
#     python_callable=fetch_categories,
#     dag=dag
# )
#
# transfer_order_details_task = PythonOperator(
#     task_id='transfer_order_details',
#     python_callable=transfer_order_details,
#     dag=dag
# )
#
# clean_order_details_task = PythonOperator(
#     task_id='clean_order_details',
#     python_callable=clean_order_details,
#     dag=dag
# )
#
# fetch_order_details_task = PythonOperator(
#     task_id='fetch_order_details',
#     python_callable=fetch_order_details,
#     dag=dag
# )
#
# [transfer_suppliers_task << clean_suppliers_task << fetch_suppliers_task,
#  transfer_orders_task << clean_orders_task << fetch_orders_task,
#  transfer_employees_task << clean_employees_task << fetch_employees_task,
#  transfer_customers_task << clean_customers_task << fetch_customers_task,
#  transfer_products_task << clean_products_task << fetch_products_task,
#  transfer_categories_task << clean_categories_task << fetch_categories_task,
#  transfer_order_details_task << clean_order_details_task << fetch_order_details_task] << truncate_tables_task
