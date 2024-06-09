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


def to_customer_dimension():
    source_sql = "SELECT customer_id, company_name, city, country FROM customers;"

    destination_sql = "INSERT INTO customer (customer_id, company_name, city, country) VALUES (%s, %s, %s, %s)"

    _transfer_records(source_sql, destination_sql)


def to_supplier_dimension():
    source_sql = "SELECT supplier_id, company_name, country FROM suppliers;"

    destination_sql = "INSERT INTO supplier (supplier_id, company_name, country) VALUES (%s, %s, %s)"

    _transfer_records(source_sql, destination_sql)


def to_time_dimension():
    source_sql = """
    SELECT DISTINCT 
    order_id AS time_id, 
    order_date AS date, 
    date_part('year', order_date) AS year, 
    date_part('quarter', order_date) AS quarter, date_part('month', order_date) AS month, 
    date_part('day', order_date) AS day 
    FROM orders;
    """

    destination_sql = "INSERT INTO time (time_id, date, year, quarter, month, day) VALUES (%s, %s, %s, %s, %s, %s)"

    _transfer_records(source_sql, destination_sql)


def to_product_dimension():
    source_sql = """
    SELECT product_id, product_name, category_name AS product_category
    FROM products 
    JOIN categories c ON c.category_id = products.category_id;
    """

    destination_sql = "INSERT INTO product (product_id, product_name, product_category) VALUES (%s, %s, %s)"

    _transfer_records(source_sql, destination_sql)


def to_employee_dimension():
    source_sql = """
    SELECT emp.employee_id, emp.last_name AS emp_last_name, mng.last_name AS mng_last_name 
    FROM employees emp 
    LEFT JOIN employees mng on emp.reports_to = mng.employee_id;
    """

    destination_sql = "INSERT INTO employee (employee_id, emp_last_name, mng_last_name) VALUES (%s, %s, %s)"

    _transfer_records(source_sql, destination_sql)


def to_order_facts():
    source_sql = """
    SELECT customer_id, od.product_id, employee_id, od.order_id AS time_id, supplier_id, od.unit_price, quantity 
    FROM order_details od 
    JOIN orders o ON od.order_id = o.order_id 
    JOIN products p ON od.product_id = p.product_id;
    """

    destination_sql = """
    INSERT INTO orders_facts (customer_id, product_id, employee_id, time_id, supplier_id, price, quantity) 
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    _transfer_records(source_sql, destination_sql)


def _transfer_records(source_sql, destination_sql):
    source_hook = PostgresHook(postgres_conn_id='postgres_source')
    star_hook = PostgresHook(postgres_conn_id='postgres_star')

    source_data = source_hook.get_records(source_sql)
    if source_data:
        for row in source_data:
            star_hook.run(destination_sql, parameters=row)


customer_task = PythonOperator(
    task_id='to_customer_dimension',
    python_callable=to_customer_dimension,
    dag=dag
)

supplier_task = PythonOperator(
    task_id='to_supplier_dimension',
    python_callable=to_supplier_dimension,
    dag=dag
)

time_task = PythonOperator(
    task_id='to_time_dimension',
    python_callable=to_time_dimension,
    dag=dag
)

product_task = PythonOperator(
    task_id='to_product_dimension',
    python_callable=to_product_dimension,
    dag=dag
)

employee_task = PythonOperator(
    task_id='to_employee_dimension',
    python_callable=to_employee_dimension,
    dag=dag
)

order_task = PythonOperator(
    task_id='to_order_facts',
    python_callable=to_order_facts,
    dag=dag
)
