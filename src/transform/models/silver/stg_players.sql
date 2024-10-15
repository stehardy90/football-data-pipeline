WITH raw_players AS (
    SELECT
        JSON_EXTRACT_SCALAR(player_data, '$.id') AS player_id,
        JSON_EXTRACT_SCALAR(team_data, '$.id') AS team_id,
        JSON_EXTRACT_SCALAR(player_data, '$.name') AS player_name,
        COALESCE(JSON_EXTRACT_SCALAR(player_data, '$.position'), 'Unknown') AS player_position,
        JSON_EXTRACT_SCALAR(player_data, '$.dateOfBirth') AS player_date_of_birth,
        JSON_EXTRACT_SCALAR(player_data, '$.nationality') AS player_nationality,
        ROW_NUMBER() OVER (PARTITION BY JSON_EXTRACT_SCALAR(player_data, '$.id') ORDER BY loaded_date DESC) AS row_num
    FROM `football-data-pipeline.football_data_bronze.raw_football_teams`,
    UNNEST(JSON_EXTRACT_ARRAY(raw_json, '$.teams')) AS team_data,  -- Unnest the teams array first
    UNNEST(JSON_EXTRACT_ARRAY(team_data, '$.squad')) AS player_data  -- Then unnest the squad array inside teams
)

SELECT
    player_id,
    team_id,
    player_name,
    player_position,
    player_date_of_birth,
    player_nationality,
    CURRENT_TIMESTAMP() AS loaded_date
FROM raw_players
WHERE row_num = 1
