WITH raw_scorers AS (
    SELECT
        JSON_EXTRACT_SCALAR(scorer_data, '$.player.id') AS player_id,
        JSON_EXTRACT_SCALAR(scorer_data, '$.team.id') AS team_id,
        JSON_EXTRACT_SCALAR(scorer_data, '$.playedMatches') AS played_matches,
        JSON_EXTRACT_SCALAR(scorer_data, '$.goals') AS goals,
        COALESCE(JSON_EXTRACT_SCALAR(scorer_data, '$.assists'), '0') AS assists,
        COALESCE(JSON_EXTRACT_SCALAR(scorer_data, '$.penalties'), '0') AS penalties,
        JSON_EXTRACT_SCALAR(raw_json, '$.competition.id') AS competition_id,
        JSON_EXTRACT_SCALAR(raw_json, '$.season.id') AS season_id,
        JSON_EXTRACT_SCALAR(raw_json, '$.season.currentMatchday') AS current_matchday,
        ROW_NUMBER() OVER (
            PARTITION BY 
                JSON_EXTRACT_SCALAR(scorer_data, '$.player.id'), 
                JSON_EXTRACT_SCALAR(scorer_data, '$.team.id'), 
                JSON_EXTRACT_SCALAR(raw_json, '$.competition.id'), 
                JSON_EXTRACT_SCALAR(raw_json, '$.season.id')
            ORDER BY loaded_date DESC
        ) AS row_num
    FROM `football-data-pipeline.football_data_bronze.raw_football_scorers`,
    UNNEST(JSON_EXTRACT_ARRAY(raw_json, '$.scorers')) AS scorer_data 
)

SELECT
    player_id,
    team_id,
    played_matches,
    goals,
    assists,
    penalties,
    competition_id,
    season_id,
    current_matchday,
    CURRENT_TIMESTAMP() AS loaded_date
FROM raw_scorers
WHERE row_num = 1