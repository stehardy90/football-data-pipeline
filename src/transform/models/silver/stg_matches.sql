{{
  config(
    materialized='incremental',  
    unique_key='match_id'  
  )
}}

WITH raw_matches AS (
    SELECT
        CAST(JSON_EXTRACT_SCALAR(match_data, '$.id') AS INT64) AS match_id,
        CAST(JSON_EXTRACT_SCALAR(match_data, '$.homeTeam.id') AS INT64) AS home_team_id,
        CAST(JSON_EXTRACT_SCALAR(match_data, '$.awayTeam.id') AS INT64) AS away_team_id,
        CAST(COALESCE(JSON_EXTRACT_SCALAR(match_data, '$.score.fullTime.home'), '0') AS INT64) AS score_home,
        CAST(COALESCE(JSON_EXTRACT_SCALAR(match_data, '$.score.fullTime.away'), '0') AS INT64) AS score_away,
        CAST(COALESCE(JSON_EXTRACT_SCALAR(match_data, '$.score.halfTime.home'), '0') AS INT64) AS half_time_home,
        CAST(COALESCE(JSON_EXTRACT_SCALAR(match_data, '$.score.halfTime.away'), '0') AS INT64) AS half_time_away,
        JSON_EXTRACT_SCALAR(match_data, '$.status') AS match_status,
        CAST(JSON_EXTRACT_SCALAR(match_data, '$.utcDate') AS TIMESTAMP) AS match_day,
        CAST(JSON_EXTRACT_SCALAR(raw_json, '$.competition.id') AS INT64) AS competition_id,
        CAST(JSON_EXTRACT_SCALAR(match_data, '$.season.id') AS INT64) AS season_id,
        CAST(JSON_EXTRACT_SCALAR(ref_data, '$.id') AS INT64) AS referee_id,
        ROW_NUMBER() OVER (PARTITION BY JSON_EXTRACT_SCALAR(match_data, '$.id') ORDER BY loaded_date DESC) AS row_num
    FROM `{{ var('bigquery_dataset') }}.raw_football_matches`,
    UNNEST(JSON_EXTRACT_ARRAY(raw_json, '$.matches')) AS match_data,  
    UNNEST(JSON_EXTRACT_ARRAY(match_data, '$.referees')) AS ref_data  
)

SELECT
    match_id,
    home_team_id,
    away_team_id,
    score_home,
    score_away,
    half_time_home,
    half_time_away,
    match_status,
    match_day,
    competition_id,
    season_id,
    referee_id,
    CURRENT_TIMESTAMP() AS loaded_date
FROM raw_matches
WHERE row_num = 1

{% if is_incremental() %}
  AND match_day > (SELECT MAX(match_day) FROM {{ this }})
{% endif %}
