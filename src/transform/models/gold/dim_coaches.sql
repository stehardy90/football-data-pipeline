SELECT
    coach_id,
    coach_name,
    coach_date_of_birth,
    coach_nationality,
    coach_contract_start,
    coach_contract_until,
    team_id,
    dbt_valid_from as valid_from,
    ifnull(dbt_valid_to,'2099-12-31') as valid_to,    
    CURRENT_TIMESTAMP() AS loaded_date   
FROM
    {{ ref('snp_coaches') }}