SELECT
  Restaurant_Index,
  Societe,
  UUID,
  Plateforme,
  Date_signature,
  COMPTE_FERME,
  JAMAIS_LANCE,
  CHURN,
  OB,
  DATE_CHURN
FROM {{ source('brouillon', 'customer_table')}}
