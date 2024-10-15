SELECT
    competition_id,
    competition_name,
    competition_code,
    competition_type,
    competition_emblem,
    area_code,
    dbt_valid_from as valid_from,
    ifnull(dbt_valid_to,'2099-12-31') as valid_to,    
    CURRENT_TIMESTAMP() AS loaded_date   
FROM
    football-data-pipeline.football_data_snapshot.snp_competitions