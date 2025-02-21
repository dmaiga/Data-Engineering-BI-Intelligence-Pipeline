
WITH ProduitCounts AS (
  SELECT produit_id, 
          produit_nom,
           categorie, 
           sous_categorie, 
           COUNT(*) AS cnt
  FROM {{ ref('stg_ecommerce_data') }}
  GROUP BY produit_id, produit_nom, categorie, sous_categorie
),
BestProduct AS (
  SELECT produit_id,
         produit_nom,
          categorie, 
          sous_categorie
  FROM (
    SELECT produit_id, produit_nom, categorie, sous_categorie,
           ROW_NUMBER() OVER (PARTITION BY produit_id ORDER BY cnt DESC) AS rn
    FROM ProduitCounts
  ) t
  WHERE rn = 1
)
SELECT * FROM BestProduct
