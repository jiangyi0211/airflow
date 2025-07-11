-- dim_customer.sql

-- Create the dimension table
WITH customer_cte AS (
	SELECT DISTINCT
	    to_hex(md5(cast(coalesce(cast(CustomerID as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Country as string), '_dbt_utils_surrogate_key_null_') as string))) as customer_id,
	    Country AS country
	FROM `airflow-dataonline`.`retail`.`raw_invoices`
	WHERE CustomerID IS NOT NULL
)
SELECT
    t.*,
	cm.iso
FROM customer_cte t
LEFT JOIN `airflow-dataonline`.`retail`.`country` cm ON t.country = cm.nicename