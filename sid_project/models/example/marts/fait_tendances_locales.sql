WITH AggregatedData AS (
    SELECT
        city,                               
        EXTRACT(YEAR FROM date_commande) AS year,  
        EXTRACT(MONTH FROM date_commande) AS month,
        produit_id AS product_id,         
        SUM(montant_vente) AS total_sales,
        SUM(quantite) AS total_quantity   
    FROM {{ ref('fait_ventes') }}
    GROUP BY city, year, month, product_id
),
TopProducts AS (
    SELECT
        city,
        year,
        month,
        product_id AS productid_phare,     -- Produit phare (meilleure vente)
        total_sales,
        total_quantity,
        ROW_NUMBER() OVER (
            PARTITION BY city, year, month
            ORDER BY total_sales DESC
        ) AS rank                          -- Identifier le produit phare
    FROM AggregatedData
)
SELECT
    city,
    year,
    month,
    productid_phare,
    total_sales,
    total_quantity
FROM TopProducts
WHERE rank = 1                         -- Ne garder que le produit phare
