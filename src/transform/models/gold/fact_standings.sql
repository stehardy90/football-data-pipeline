 SELECT                                                                                 
    league_position,
    team_id,
    played_games as games_played,
    won,
    draw as drew,
    lost,
    points,
    goals_for,
    goals_against,
    goal_difference,
	competition_id,
	season_id,
    dbt_valid_from as valid_from,
    ifnull(dbt_valid_to,'2099-12-31') as valid_to,
    CURRENT_TIMESTAMP() AS loaded_date
FROM 
    {{ ref('snp_standings') }}
