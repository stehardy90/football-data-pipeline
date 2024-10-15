SELECT
	position
FROM 
	football-data-pipeline.football_data_aggregate.fact_standings
WHERE 
	goal_difference != (goals_for - goals_against)
	OR points > (games_played * 3)
	OR won > games_played
	OR lost > games_played
	OR drew > games_played

