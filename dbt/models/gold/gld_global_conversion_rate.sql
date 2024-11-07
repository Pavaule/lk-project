WITH restaurant_metrics AS (
  SELECT
    UUID,
    AVG(conversion_rate) as avg_conversion_rate,
    MIN(conversion_rate) as min_conversion_rate,
    MAX(conversion_rate) as max_conversion_rate,
    COUNT(*) as number_of_weeks,
    MIN(weekStart) as first_week,
    MAX(weekStart) as last_week
  FROM {{ ref('slv_all_platform_conversion_rate') }}
  GROUP BY
    UUID
),

global_metrics AS (
  SELECT
    AVG(conversion_rate) as lk_conversion_rate
  FROM {{ ref('slv_all_platform_conversion_rate') }}
)

SELECT
  rm.*,
  DATE_DIFF(last_week, first_week, WEEK) + 1 as weeks_of_history,
  (SELECT lk_conversion_rate FROM global_metrics) as lk_conversion_rate,
  (rm.avg_conversion_rate - (SELECT lk_conversion_rate FROM global_metrics)) as diff_from_lk_avg
FROM restaurant_metrics rm
ORDER BY
  avg_conversion_rate DESC
