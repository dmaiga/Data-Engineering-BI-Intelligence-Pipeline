SELECT DISTINCT
    client_id,
    client_nom,
    segment
FROM {{ ref('stg_ecommerce_data') }}

