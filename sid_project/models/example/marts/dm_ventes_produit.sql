WITH ventes_agg AS (
    SELECT 
        f.produit_id,
        p.produit_nom,
        p.categorie,
        t.mois,
        t.annee,
        SUM(f.quantite) AS quantite_totale,
        SUM(f.montant_vente) AS revenu_total,
        SUM(f.profit) AS profit_total
    FROM {{ ref('fait_ventes') }} f
    JOIN {{ ref('dim_produit') }} p ON f.produit_id = p.produit_id
    JOIN {{ ref('dim_temps') }} t ON f.date_commande = t.date_commande
    GROUP BY f.produit_id, p.produit_nom, p.categorie, t.mois, t.annee
)
SELECT * FROM ventes_agg
