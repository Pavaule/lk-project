WITH weekly_global_avg AS (
  SELECT
    weekStart,
    AVG(conversion_rate) as global_weekly_conversion_rate
  FROM {{ ref('slv_all_platform_conversion_rate') }}
  GROUP BY
    weekStart
)

SELECT
    s.*,  -- Garde toutes les colonnes de la table silver
    w.global_weekly_conversion_rate
FROM {{ ref('slv_all_platform_conversion_rate') }} s
LEFT JOIN weekly_global_avg w
    ON s.weekStart = w.weekStart
