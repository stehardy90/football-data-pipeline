WITH raw_scorers AS (
    SELECT
        CAST(JSON_EXTRACT_SCALAR(scorer_data, '$.player.id') AS INT64) AS player_id,
        CAST(JSON_EXTRACT_SCALAR(scorer_data, '$.team.id') AS INT64) AS team_id,
        CAST(JSON_EXTRACT_SCALAR(scorer_data, '$.playedMatches') AS INT64) AS played_matches,
        CAST(JSON_EXTRACT_SCALAR(scorer_data, '$.goals') AS INT64) AS goals,
        CAST(COALESCE(JSON_EXTRACT_SCALAR(scorer_data, '$.assists'), '0') AS INT64) AS assists,
        CAST(COALESCE(JSON_EXTRACT_SCALAR(scorer_data, '$.penalties'), '0') AS INT64) AS penalties,
        CAST(JSON_EXTRACT_SCALAR(raw_json, '$.competition.id') AS INT64) AS competition_id,
        CAST(JSON_EXTRACT_SCALAR(raw_json, '$.season.id') AS INT64) AS season_id,
        CAST(JSON_EXTRACT_SCALAR(raw_json, '$.season.currentMatchday') AS INT64) AS current_matchday,
        ROW_NUMBER() OVER (
            PARTITION BY 
                JSON_EXTRACT_SCALAR(scorer_data, '$.player.id'), 
                JSON_EXTRACT_SCALAR(scorer_data, '$.team.id'), 
                JSON_EXTRACT_SCALAR(raw_json, '$.competition.id'), 
                JSON_EXTRACT_SCALAR(raw_json, '$.season.id')
            ORDER BY loaded_date DESC
        ) AS row_num
    FROM `{{ var('bigquery_dataset') }}.raw_football_scorers`,
    UNNEST(JSON_EXTRACT_ARRAY(raw_json, '$.scorers')) AS scorer_data 
	WHERE JSON_EXTRACT_SCALAR(scorer_data, '$.player.id') IS NOT NULL
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