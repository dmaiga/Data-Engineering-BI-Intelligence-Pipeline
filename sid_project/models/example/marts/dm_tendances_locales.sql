WITH AggregatedSales AS (
    SELECT 
        f.city,
        d.mois,
        f.produit_id,
        SUM(f.profit) AS total_ventes,
        SUM(f.quantite) AS quantite_vendue
    FROM {{ ref('fait_ventes') }} f
    JOIN {{ ref('dim_temps') }} d ON f.date_commande = d.date_commande
    GROUP BY f.city, d.mois, f.produit_id
),
BestProduct AS (
    SELECT 
        city,
        mois,
        produit_id AS productid_phare,
        total_ventes,
        quantite_vendue,
        ROW_NUMBER() OVER (PARTITION BY city, mois ORDER BY total_ventes DESC) AS rn
    FROM AggregatedSales
)
SELECT 
    city,
    mois,
    productid_phare,
    total_ventes,
    quantite_vendue
FROM BestProduct
WHERE rn = 1
