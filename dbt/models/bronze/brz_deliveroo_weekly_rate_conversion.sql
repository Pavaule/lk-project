SELECT
  restaurant_id,
  starting_date,
  ending_date,
  conversion_rate_order_count_divide_by_menu_view_count
FROM {{ source('brouillon', 'deliveroo_weekly_conversion_rate_and_customer_retention')}}
