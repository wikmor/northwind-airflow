from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from faker import Faker

from utils.baseutil import SourceUtil
from utils.baseutil import StagingUtil
from utils.baseutil import StarUtil
from entities import entities, entities_load

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
    'etl_dag',
    default_args=default_args,
    description='Clean and transfer data between source and staging databases',
    schedule_interval='@daily',
    catchup=False
)


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
        return SourceUtil.get_records(self.fetch_sql)

    def clean_data(self, data):
        return self.clean_func(data)


class Load:
    def __init__(
            self,
            entity: str,
            fetch_sql: str,
            insert_sql: str
    ) -> None:
        self.entity = entity
        self.fetch_sql = fetch_sql
        self.insert_sql = insert_sql

    def transfer_data(self):
        data = self.fetch_data()
        StarUtil.query_multiple(self.insert_sql, data)

    def fetch_data(self):
        return StagingUtil.get_records(self.fetch_sql)


def truncate_tables():
    StagingUtil.query("""
    TRUNCATE TABLE order_details_tmp, products_tmp, categories_tmp, customers_tmp, employees_tmp, suppliers_tmp, orders_tmp;
    """)
    StarUtil.query("""
    TRUNCATE TABLE orders_facts, employee, supplier, product, customer, time;
    """)


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
customers_transfer_task = next(task for task in transfer_tasks if task.task_id == 'transfer_customers')
employees_transfer_task = next(task for task in transfer_tasks if task.task_id == 'transfer_employees')

# Ensure orders_transfer_task runs after customers_transfer_task and employees_transfer_task
orders_transfer_task.set_upstream(customers_transfer_task)
orders_transfer_task.set_upstream(employees_transfer_task)

# Ensure products_transfer_task runs after categories_transfer_task and suppliers_transfer_task
products_transfer_task.set_upstream(categories_transfer_task)
products_transfer_task.set_upstream(suppliers_transfer_task)

# Ensure order_details_transfer_task runs after products_transfer_task and orders_transfer_task
order_details_transfer_task.set_upstream(products_transfer_task)
order_details_transfer_task.set_upstream(orders_transfer_task)

truncate_tables_task >> fetch_tasks

# Add the Load tasks
load_time_task = PythonOperator(
    task_id='load_time',
    python_callable=entities_load['time'].transfer_data,
    dag=dag
)

load_supplier_task = PythonOperator(
    task_id='load_supplier',
    python_callable=entities_load['supplier'].transfer_data,
    dag=dag
)

load_product_task = PythonOperator(
    task_id='load_product',
    python_callable=entities_load['product'].transfer_data,
    dag=dag
)

load_employee_task = PythonOperator(
    task_id='load_employee',
    python_callable=entities_load['employee'].transfer_data,
    dag=dag
)

load_customer_task = PythonOperator(
    task_id='load_customer',
    python_callable=entities_load['customer'].transfer_data,
    dag=dag
)

load_orders_facts_task = PythonOperator(
    task_id='load_orders_facts',
    python_callable=entities_load['orders_facts'].transfer_data,
    dag=dag
)

# Ensure load tasks run after the corresponding transform tasks
order_details_transfer_task >> load_time_task
suppliers_transfer_task >> load_supplier_task
products_transfer_task >> load_product_task
orders_transfer_task >> load_employee_task
customers_transfer_task >> load_customer_task

# Ensure orders_facts load task runs after all necessary dependencies
load_supplier_task >> load_orders_facts_task
load_product_task >> load_orders_facts_task
load_employee_task >> load_orders_facts_task
load_customer_task >> load_orders_facts_task
load_time_task >> load_orders_facts_task
