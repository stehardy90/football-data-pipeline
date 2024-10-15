SELECT
    coach_id
FROM 
    football-data-pipeline.football_data_aggregate.dim_coaches
WHERE 
    contract_start >= contract_until
