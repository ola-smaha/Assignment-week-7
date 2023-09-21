CREATE TABLE IF NOT EXISTS dw_reporting.dim_customer
(
    ID SERIAL PRIMARY KEY NOT NULL,
    customer_name TEXT,
    customer_lastname TEXT,
    updated_date TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_customer_id ON dw_reporting.dim_customer(ID);

TRUNCATE TABLE dw_reporting.dim_customer;
INSERT INTO dw_reporting.dim_customer 
(
    SELECT 
        customer_id AS ID,
        first_name AS customer_name,
        last_name AS customer_lastname,
        last_update AS updated_date
    FROM dw_reporting.stg_dvd_rental_customer
)

