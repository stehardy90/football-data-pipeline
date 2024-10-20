{{
  config(
    materialized='incremental',  -- This sets the model to incremental
    unique_key='match_id'  -- Ensure match_id is the unique key for incremental updates
  )
}}

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
FROM 
    {{ ref('stg_matches') }}
	
{% if is_incremental() %}
WHERE match_day > (SELECT MAX(match_day) FROM {{ this }})
{% endif %}