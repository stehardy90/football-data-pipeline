SELECT
    player_id,
    player_name,
    player_position,
    player_date_of_birth,
    player_nationality,
    team_id,
    dbt_valid_from as valid_from,
    ifnull(dbt_valid_to,'2099-12-31') as valid_to,    
    CURRENT_TIMESTAMP() AS loaded_date   
FROM
    {{ ref('snp_players') }}