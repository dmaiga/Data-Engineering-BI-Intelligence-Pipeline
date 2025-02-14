SELECT DISTINCT
    date_commande,
    EXTRACT(YEAR FROM date_commande) AS annee,
    EXTRACT(MONTH FROM date_commande) AS mois,
    EXTRACT(DAY FROM date_commande) AS jour
FROM {{ ref('stg_ecommerce_data') }}
