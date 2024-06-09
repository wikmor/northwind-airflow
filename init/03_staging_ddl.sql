\c staging;

CREATE TABLE suppliers (
    supplier_id  smallint NOT NULL PRIMARY KEY,
    company_name character varying(40) NOT NULL,
    country      character varying(15)
);
