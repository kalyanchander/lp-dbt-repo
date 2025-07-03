{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='merge',
    on_schema_change='fail'
) }}

SELECT
  customer_id,
  customer_name,
  email,
  created_at

FROM AIRBNB.RAW.CUSTOMERS  -- Using source for direct table reference

{% if is_incremental() %}
  -- Only select new or updated rows since the last run
  WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}

