SELECT
    season_id
FROM 
    football-data-pipeline.football_data_aggregate.dim_seasons
WHERE 
    season_start_date >= season_end_date

