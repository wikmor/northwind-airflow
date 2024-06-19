from transform_data import Transform
from load_data import Load

from faker import Faker

fake = Faker()


def clean_suppliers(data):
    return [(supplier_id, fake.company(), country if country is not None else 'NoCountryProvided')
            for supplier_id, company_name, country in data]


def clean_orders(data):
    return [(order_id, customer_id, employee_id, order_date) for order_id, customer_id, employee_id, order_date in data]


def clean_employees(data):
    return [(employee_id, last_name, first_name, reports_to)
            for employee_id, last_name, first_name, reports_to in data]


def clean_customers(data):
    return [(customer_id, company_name, city, country)
            for customer_id, company_name, city, country in data]


def clean_products(data):
    return [(product_id, product_name, supplier_id, category_id)
            for product_id, product_name, supplier_id, category_id in data]


def clean_categories(data):
    return [(category_id, category_name) for category_id, category_name in data]


def clean_order_details(data):
    return [(order_id, product_id, unit_price, quantity)
            for order_id, product_id, unit_price, quantity in data]


entities = {
    'suppliers': Transform(
        entity='suppliers',
        fetch_sql="SELECT supplier_id, company_name, country FROM suppliers;",
        insert_sql="INSERT INTO suppliers_tmp (supplier_id, company_name, country) VALUES (%s, %s, %s)",
        clean_func=clean_suppliers
    ),
    'orders': Transform(
        entity='orders',
        fetch_sql="SELECT order_id, customer_id, employee_id, order_date FROM orders;",
        insert_sql="INSERT INTO orders_tmp (order_id, customer_id, employee_id, order_date) VALUES (%s, %s, %s, %s)",
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

entities_load = {
    'time': Load(
        entity='time',
        fetch_sql="""
        SELECT DISTINCT
        order_id AS time_id,
        order_date AS date,
        date_part('year', order_date) AS year,
        date_part('quarter', order_date) AS quarter, date_part('month', order_date) AS month,
        date_part('day', order_date) AS day
        FROM orders_tmp;
        """,
        insert_sql="INSERT INTO time (time_id, date, year, quarter, month, day) VALUES (%s, %s, %s, %s, %s, %s)"
    ),
    'supplier': Load(
        entity='supplier',
        fetch_sql="SELECT supplier_id, company_name, country FROM suppliers_tmp;",
        insert_sql="INSERT INTO supplier (supplier_id, company_name, country) VALUES (%s, %s, %s)"
    ),
    'product': Load(
        entity='product',
        fetch_sql="""
        SELECT product_id, product_name, category_name AS product_category
        FROM products_tmp
        JOIN categories_tmp c ON c.category_id = products_tmp.category_id;
        """,
        insert_sql="INSERT INTO product (product_id, product_name, product_category) VALUES (%s, %s, %s)"
    ),
    'employee': Load(
        entity='employee',
        fetch_sql="""
        SELECT emp.employee_id, emp.last_name AS emp_last_name, mng.last_name AS mng_last_name
        FROM employees_tmp emp
        LEFT JOIN employees_tmp mng on emp.reports_to = mng.employee_id;
        """,
        insert_sql="INSERT INTO employee (employee_id, emp_last_name, mng_last_name) VALUES (%s, %s, %s)"
    ),
    'customer': Load(
        entity='customer',
        fetch_sql="SELECT customer_id, company_name, city, country FROM customers_tmp;"
        ,
        insert_sql="INSERT INTO customer (customer_id, company_name, city, country) VALUES (%s, %s, %s, %s)"
    ),
    'orders_facts': Load(
        entity='orders_facts',
        fetch_sql="""
        SELECT customer_id, od.product_id, employee_id, od.order_id AS time_id, supplier_id, od.unit_price, quantity
        FROM order_details_tmp od
        JOIN orders_tmp o ON od.order_id = o.order_id
        JOIN products_tmp p ON od.product_id = p.product_id;
        """,
        insert_sql="""
        INSERT INTO orders_facts (customer_id, product_id, employee_id, time_id, supplier_id, price, quantity)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
    ),
}
