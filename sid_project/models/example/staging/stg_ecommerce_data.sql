WITH raw_data AS (
    SELECT 
        * 
    FROM `sid-ecommerce-450600.ecommerce_raw_data.ecommerce_data`
)
SELECT
    `Order ID` AS commande_id,
    `Product ID` AS produit_id,
    `Customer ID` AS client_id,
    `Order Date` AS date_commande,
    `Sales` AS montant_vente,
    `Quantity` AS quantite,
    `Discount` AS remise,
    `Profit` AS profit,
    `Product Name` AS produit_nom,
    `Category` AS categorie,
    `Sub-Category` AS sous_categorie,
    `Customer Name` AS client_nom,
    `City` AS city,
    `State` AS state
FROM raw_data
