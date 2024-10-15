WITH standings_staging AS (
    SELECT
        JSON_EXTRACT_SCALAR(team_data, '$.team.id') AS team_id,
        JSON_EXTRACT_SCALAR(team_data, '$.position') AS league_position,
        JSON_EXTRACT_SCALAR(team_data, '$.playedGames') AS played_games,
        JSON_EXTRACT_SCALAR(team_data, '$.won') AS won,
        JSON_EXTRACT_SCALAR(team_data, '$.draw') AS draw,
        JSON_EXTRACT_SCALAR(team_data, '$.lost') AS lost,
        JSON_EXTRACT_SCALAR(team_data, '$.points') AS points,
        JSON_EXTRACT_SCALAR(team_data, '$.goalsFor') AS goals_for,
        JSON_EXTRACT_SCALAR(team_data, '$.goalsAgainst') AS goals_against,
        JSON_EXTRACT_SCALAR(team_data, '$.goalDifference') AS goal_difference,
        JSON_EXTRACT_SCALAR(raw_json, '$.competition.id') AS competition_id,
        JSON_EXTRACT_SCALAR(raw_json, '$.season.id') AS season_id,
        ROW_NUMBER() OVER (PARTITION BY JSON_EXTRACT_SCALAR(team_data, '$.team.id') ORDER BY loaded_date DESC) AS row_num
    FROM `football-data-pipeline.football_data_bronze.raw_football_standings`,
    UNNEST(JSON_EXTRACT_ARRAY(raw_json, '$.standings')) AS standing_data,  -- Unnest standings
    UNNEST(JSON_EXTRACT_ARRAY(standing_data, '$.table')) AS team_data  -- Unnest table inside standings
)

SELECT
    team_id,
    league_position,
    played_games,
    won,
    draw,
    lost,
    points,
    goals_for,
    goals_against,
    goal_difference,
    competition_id,
    season_id,
    CURRENT_TIMESTAMP() AS loaded_date
FROM standings_staging
WHERE row_num = 1
