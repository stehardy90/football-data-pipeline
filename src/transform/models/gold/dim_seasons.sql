SELECT 
    season_id,
    season_start_date,
    season_end_date,
    current_matchday,
    competition_id,
    dbt_valid_from as valid_from,
    ifnull(dbt_valid_to,'2099-12-31') as valid_to,    
    CURRENT_TIMESTAMP() AS loaded_date
FROM 
    {{ ref('snp_seasons') }}

