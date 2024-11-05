SELECT
  restaurant_id,
  starting_date,
  ending_date,
  conversion_rate
FROM {{ source('deliveroo','deliveroo_weekly_conversion_rate_and_customer_retention')}}
