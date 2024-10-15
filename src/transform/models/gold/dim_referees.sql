SELECT 
    referee_id,
    referee_name,
    referee_nationality,
    dbt_valid_from as valid_from,
    ifnull(dbt_valid_to,'2099-12-31') as valid_to,    
    CURRENT_TIMESTAMP() AS loaded_date
FROM 
    {{ ref('snp_referees') }}
