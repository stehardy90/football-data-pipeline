SELECT
    coach_id
FROM 
    football-data-pipeline.football_data_aggregate.dim_coaches
WHERE 
    date_of_birth >= DATE_SUB(CURRENT_DATE(), INTERVAL 10 YEAR)


UNION ALL

SELECT
    player_id
FROM 
    football-data-pipeline.football_data_aggregate.dim_players
WHERE 
    date_of_birth >= DATE_SUB(CURRENT_DATE(), INTERVAL 10 YEAR)
