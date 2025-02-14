SELECT DISTINCT
    client_id,
    client_nom
FROM {{ ref('stg_ecommerce_data') }}

