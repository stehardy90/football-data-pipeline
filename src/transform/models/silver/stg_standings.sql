WITH standings_staging AS (
    SELECT
        CAST(JSON_EXTRACT_SCALAR(team_data, '$.team.id') AS INT64) AS team_id,
        CAST(JSON_EXTRACT_SCALAR(team_data, '$.position') AS INT64) AS league_position,
        CAST(JSON_EXTRACT_SCALAR(team_data, '$.playedGames') AS INT64) AS played_games,
        CAST(JSON_EXTRACT_SCALAR(team_data, '$.won') AS INT64) AS won,
        CAST(JSON_EXTRACT_SCALAR(team_data, '$.draw') AS INT64) AS draw,
        CAST(JSON_EXTRACT_SCALAR(team_data, '$.lost') AS INT64) AS lost,
        CAST(JSON_EXTRACT_SCALAR(team_data, '$.points') AS INT64) AS points,
        CAST(JSON_EXTRACT_SCALAR(team_data, '$.goalsFor') AS INT64) AS goals_for,
        CAST(JSON_EXTRACT_SCALAR(team_data, '$.goalsAgainst') AS INT64) AS goals_against,
        CAST(JSON_EXTRACT_SCALAR(team_data, '$.goalDifference') AS INT64) AS goal_difference,
        CAST(JSON_EXTRACT_SCALAR(raw_json, '$.competition.id') AS INT64) AS competition_id,
        CAST(JSON_EXTRACT_SCALAR(raw_json, '$.season.id') AS INT64) AS season_id,
        ROW_NUMBER() OVER (PARTITION BY JSON_EXTRACT_SCALAR(team_data, '$.team.id') ORDER BY loaded_date DESC) AS row_num
    FROM `{{ var('bigquery_dataset') }}.raw_football_standings`,
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
