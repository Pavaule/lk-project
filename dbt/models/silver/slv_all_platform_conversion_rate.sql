WITH deliveroo_conversion AS (
  SELECT
    CAST(restaurant_id AS STRING) AS restaurant_id,
    starting_date AS weekStart,
    ending_date AS weekEnd,
    conversion_rate_order_count_divide_by_menu_view_count AS conversion_rate,
    'Deliveroo' AS platform,
    EXTRACT(WEEK FROM starting_date) AS week_number,
    EXTRACT(YEAR FROM starting_date) AS year
  FROM {{ref('brz_deliveroo_weekly_rate_conversion')}}
),

uber_conversion AS (
  SELECT
    restaurantUuid AS restaurant_id,
    weekStart,
    weekEnd,
    `Taux conversion` AS conversion_rate,
    'Uber' AS platform,
    EXTRACT(WEEK FROM weekStart) AS week_number,
    EXTRACT(YEAR FROM weekStart) AS year
  FROM {{ref('brz_uber_menu_weekly_rate_conversion')}}
),

all_conversions AS (
  SELECT * FROM deliveroo_conversion
  UNION ALL
  SELECT * FROM uber_conversion
),

customer_data AS (
  SELECT
    UUID,
    Restaurant_Index,
    Societe,
    Plateforme,
    Date_signature,
    COMPTE_FERME,
    JAMAIS_LANCE,
    CHURN,
    OB,
    DATE_CHURN
  FROM {{ ref('brz_customer_table') }}
)

SELECT
  c.*,
  ac.weekStart,
  ac.weekEnd,
  ac.conversion_rate,
  ac.platform,
  ac.week_number,
  ac.year
FROM customer_data c
INNER JOIN all_conversions ac
  ON c.UUID = ac.restaurant_id
WHERE ac.weekStart >= c.Date_signature
