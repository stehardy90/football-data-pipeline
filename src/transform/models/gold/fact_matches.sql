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
    football-data-pipeline.football_data_silver.stg_matches