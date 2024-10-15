WITH raw_seasons AS (
    SELECT
        CAST(JSON_EXTRACT_SCALAR(season_data, '$.id') AS INT64) AS season_id,
        CAST(JSON_EXTRACT_SCALAR(raw_json, '$.id') AS INT64) AS competition_id,
        CAST(JSON_EXTRACT_SCALAR(season_data, '$.startDate') AS DATE) AS season_start_date,
        CAST(JSON_EXTRACT_SCALAR(season_data, '$.endDate') AS DATE) AS season_end_date,
        CAST(JSON_EXTRACT_SCALAR(season_data, '$.currentMatchday') AS INT64) AS current_matchday,
        row_number() OVER (PARTITION BY JSON_EXTRACT_SCALAR(season_data, '$.id') ORDER BY loaded_date DESC) AS row_num
    FROM `{{ var('bigquery_dataset') }}.raw_football_competitions`,  
    UNNEST(JSON_EXTRACT_ARRAY(raw_json, '$.seasons')) AS season_data  
)

SELECT
    season_id,
    season_start_date,
    season_end_date,
    current_matchday,
    competition_id,
    CURRENT_TIMESTAMP() AS loaded_date
FROM raw_seasons
WHERE row_num = 1
