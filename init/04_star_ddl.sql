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
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255),
    product_category VARCHAR(255)
);

-- Supplier Dimension Table
CREATE TABLE supplier (
    supplier_id INT PRIMARY KEY,
    company_name VARCHAR(255),
    Country VARCHAR(255)
);

-- Employee Dimension Table
CREATE TABLE employee (
    employee_id INT PRIMARY KEY,
    emp_last_name VARCHAR(255),
    mng_last_name VARCHAR(255)
);

-- Orders Facts Table
CREATE TABLE orders_facts (
    customer_id bpchar,
    product_id INT,
    employee_id INT,
    time_id INT,
    supplier_id INT,
    price DECIMAL(10, 2),
    quantity INT,
    PRIMARY KEY (customer_id, product_id, employee_id, time_id, supplier_id),
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id),
    FOREIGN KEY (product_id) REFERENCES product(product_id),
    FOREIGN KEY (employee_id) REFERENCES employee(employee_id),
    FOREIGN KEY (time_id) REFERENCES time(time_id),
    FOREIGN KEY (supplier_id) REFERENCES supplier(supplier_id)
);
