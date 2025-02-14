SELECT
    s.commande_id,
    p.produit_id,
    c.client_id,
    t.date_commande,
    s.montant_vente,
    s.quantite,
    s.remise,
    s.profit,
    s.city,
    s.state
FROM {{ ref('stg_ecommerce_data') }} s
LEFT JOIN {{ ref('dim_produit') }} p ON s.produit_id = p.produit_id
LEFT JOIN {{ ref('dim_client') }} c ON s.client_id = c.client_id
LEFT JOIN {{ ref('dim_temps') }} t ON s.date_commande = t.date_commande

