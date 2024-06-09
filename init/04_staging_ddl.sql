\c staging;

CREATE TABLE suppliers_tmp (
    supplier_id  smallint NOT NULL PRIMARY KEY,
    company_name character varying(255) NOT NULL,
    country      character varying(255)
);
