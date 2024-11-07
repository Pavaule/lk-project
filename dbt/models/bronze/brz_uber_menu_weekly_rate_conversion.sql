SELECT
  restaurantUuid,
  weekStart,
  weekEnd,
  `Taux conversion`
FROM {{source('brouillon', 'uber_menu_weekly_rate_conversion')}}
