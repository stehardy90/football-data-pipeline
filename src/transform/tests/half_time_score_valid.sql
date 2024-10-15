SELECT
    match_id
FROM 
    football-data-pipeline.football_data_aggregate.fact_matches
WHERE 
    half_time_away > score_away
    OR half_time_home > score_home

