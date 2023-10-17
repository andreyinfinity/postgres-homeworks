-- Create table suppliers and modify products

-- Drop table suppliers if exists

DROP TABLE IF EXISTS suppliers;

-- Create table suppliers

CREATE TABLE suppliers(
    supplier_id SERIAL NOT NULL,
    company_name character varying(50),
    contact character varying(50),
    address character varying(100),
    phone character varying(20),
    fax character varying(20),
    homepage text
);

-- Add column supplier_id into products

ALTER TABLE products
    ADD COLUMN IF NOT EXISTS supplier_id int;

-- Constraint supplier_id as primary key

ALTER TABLE suppliers
    ADD CONSTRAINT pk_suppliers PRIMARY KEY (supplier_id);

-- Foreign key

--ALTER TABLE products
--    ADD CONSTRAINT fk_products_suppliers FOREIGN KEY (supplier_id) REFERENCES suppliers;
