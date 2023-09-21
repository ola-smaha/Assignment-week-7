INSERT INTO target_schema.dim_customer
(       
    SELECT 
        customer_id AS ID,
        first_name AS customer_name,
        last_name AS customer_lastname,
        last_update AS updated_date
    FROM target_schema.stg_dvd_rental_customer
)  
ON CONFLICT(ID) DO NOTHING;