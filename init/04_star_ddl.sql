\c star;

-- Time Dimension Table
CREATE TABLE time (
    time_id smallint NOT NULL PRIMARY KEY,
    date DATE,
    year INT,
    quarter INT,
    month INT,
    day INT
);

-- Customer Dimension Table
CREATE TABLE customer (
    customer_id bpchar NOT NULL PRIMARY KEY,
    company_name character varying(40) NOT NULL,
    city character varying(15),
    country character varying(15)
);

-- Product Dimension Table
CREATE TABLE product (
    product_id smallint NOT NULL PRIMARY KEY,
    product_name character varying(40) NOT NULL,
    product_category character varying(15) NOT NULL
);

-- Supplier Dimension Table
CREATE TABLE supplier (
    supplier_id smallint NOT NULL PRIMARY KEY,
    company_name character varying(40) NOT NULL,
    country character varying(15)
);

-- Employee Dimension Table
CREATE TABLE employee (
    employee_id smallint NOT NULL PRIMARY KEY,
    emp_last_name character varying(20) NOT NULL,
    mng_last_name character varying(20)
);

-- Orders Facts Table
CREATE TABLE orders_facts (
    customer_id bpchar NOT NULL,
    product_id smallint NOT NULL,
    employee_id smallint NOT NULL,
    time_id smallint NOT NULL,
    supplier_id smallint NOT NULL,
    price real NOT NULL,
    quantity smallint NOT NULL,
    PRIMARY KEY (customer_id, product_id, employee_id, time_id, supplier_id),
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id),
    FOREIGN KEY (product_id) REFERENCES product(product_id),
    FOREIGN KEY (employee_id) REFERENCES employee(employee_id),
    FOREIGN KEY (time_id) REFERENCES time(time_id),
    FOREIGN KEY (supplier_id) REFERENCES supplier(supplier_id)
);
