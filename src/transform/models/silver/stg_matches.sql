{{
  config(
    materialized='incremental',  
    unique_key='match_id'  
  )
}}

WITH raw_matches AS (
    SELECT
        JSON_EXTRACT_SCALAR(match_data, '$.id') AS match_id,
        JSON_EXTRACT_SCALAR(match_data, '$.homeTeam.id') AS home_team_id,
        JSON_EXTRACT_SCALAR(match_data, '$.awayTeam.id') AS away_team_id,
        COALESCE(JSON_EXTRACT_SCALAR(match_data, '$.score.fullTime.home'), '0') AS score_home,
        COALESCE(JSON_EXTRACT_SCALAR(match_data, '$.score.fullTime.away'), '0') AS score_away,
        COALESCE(JSON_EXTRACT_SCALAR(match_data, '$.score.halfTime.home'), '0') AS half_time_home,
        COALESCE(JSON_EXTRACT_SCALAR(match_data, '$.score.halfTime.away'), '0') AS half_time_away,
        JSON_EXTRACT_SCALAR(match_data, '$.status') AS match_status,
        JSON_EXTRACT_SCALAR(match_data, '$.utcDate') AS match_day,
        JSON_EXTRACT_SCALAR(raw_json, '$.competition.id') AS competition_id,
        JSON_EXTRACT_SCALAR(match_data, '$.season.id') AS season_id,
        JSON_EXTRACT_SCALAR(ref_data, '$.id') AS referee_id,
        ROW_NUMBER() OVER (PARTITION BY JSON_EXTRACT_SCALAR(match_data, '$.id') ORDER BY loaded_date DESC) AS row_num
    FROM `football-data-pipeline.football_data_bronze.raw_football_matches`,
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
