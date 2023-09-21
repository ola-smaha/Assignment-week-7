CREATE TABLE IF NOT EXISTS target_schema.agg_customer
(
    ID SERIAL PRIMARY KEY NOT NULL,
    customer_name TEXT,
    customer_lastname TEXT,
    updated_date TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_customer_id ON target_schema.agg_customer(ID);