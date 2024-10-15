WITH raw_seasons AS (
    SELECT
        JSON_EXTRACT_SCALAR(season_data, '$.id') AS season_id,
        JSON_EXTRACT_SCALAR(raw_json, '$.id') AS competition_id,
        JSON_EXTRACT_SCALAR(season_data, '$.startDate') AS season_start_date,
        JSON_EXTRACT_SCALAR(season_data, '$.endDate') AS season_end_date,
        JSON_EXTRACT_SCALAR(season_data, '$.currentMatchday') AS current_matchday,
        row_number() OVER (PARTITION BY JSON_EXTRACT_SCALAR(season_data, '$.id') ORDER BY loaded_date DESC) AS row_num
    FROM `football-data-pipeline.football_data_bronze.raw_football_competitions`,  
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
