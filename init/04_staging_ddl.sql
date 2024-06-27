\c staging;

CREATE TABLE suppliers_tmp (
    supplier_id  smallint NOT NULL PRIMARY KEY,
    company_name character varying(255) NOT NULL,
    country      character varying(255)
);

CREATE TABLE employees_tmp (
    employee_id smallint NOT NULL PRIMARY KEY,
    last_name character varying(20) NOT NULL,
    first_name character varying(10) NOT NULL,
    reports_to smallint,
    FOREIGN KEY (reports_to) REFERENCES employees_tmp
);

CREATE TABLE customers_tmp (
    customer_id bpchar NOT NULL PRIMARY KEY,
    company_name character varying(40) NOT NULL,
    city character varying(15),
    country character varying(15)
);

CREATE TABLE orders_tmp (
    order_id smallint NOT NULL PRIMARY KEY,
    customer_id bpchar,
    employee_id smallint,
    order_date date NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers_tmp,
    FOREIGN KEY (employee_id) REFERENCES employees_tmp
);

CREATE TABLE categories_tmp (
    category_id smallint NOT NULL PRIMARY KEY,
    category_name character varying(15) NOT NULL
);

CREATE TABLE products_tmp (
    product_id smallint NOT NULL PRIMARY KEY,
    product_name character varying(40) NOT NULL,
    supplier_id smallint,
    category_id smallint,
    FOREIGN KEY (category_id) REFERENCES categories_tmp,
    FOREIGN KEY (supplier_id) REFERENCES suppliers_tmp
);

CREATE TABLE order_details_tmp (
    order_id smallint NOT NULL,
    product_id smallint NOT NULL,
    unit_price real NOT NULL,
    quantity smallint NOT NULL,
    PRIMARY KEY (order_id, product_id),
    FOREIGN KEY (product_id) REFERENCES products_tmp,
    FOREIGN KEY (order_id) REFERENCES orders_tmp
);
