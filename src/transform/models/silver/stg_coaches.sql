WITH raw_coaches AS (
    SELECT
        CAST(JSON_EXTRACT_SCALAR(team_data, '$.id') AS INT64) AS team_id,
        CAST(JSON_EXTRACT_SCALAR(coach_data, '$.id') AS INT64) AS coach_id,
        JSON_EXTRACT_SCALAR(coach_data, '$.name') AS coach_name,
        CAST(JSON_EXTRACT_SCALAR(coach_data, '$.dateOfBirth') AS DATE) AS coach_date_of_birth,
        JSON_EXTRACT_SCALAR(coach_data, '$.nationality') AS coach_nationality,
        
        CASE 
            WHEN JSON_EXTRACT_SCALAR(coach_data, '$.contract.start') IS NOT NULL THEN 
                PARSE_DATE('%Y-%m', JSON_EXTRACT_SCALAR(coach_data, '$.contract.start'))
            ELSE NULL
        END AS coach_contract_start,

        CASE 
            WHEN JSON_EXTRACT_SCALAR(coach_data, '$.contract.until') IS NOT NULL THEN 
                LAST_DAY(PARSE_DATE('%Y-%m', JSON_EXTRACT_SCALAR(coach_data, '$.contract.until')), MONTH)
            ELSE '9999-12-31'
        END AS coach_contract_until,

        ROW_NUMBER() OVER (PARTITION BY JSON_EXTRACT_SCALAR(team_data, '$.id') ORDER BY loaded_date DESC) AS row_num
    FROM `{{ var('bigquery_dataset') }}.raw_football_teams`,
    UNNEST(JSON_EXTRACT_ARRAY(raw_json, '$.teams')) AS team_data,  -- Unnest the teams array first
    UNNEST([JSON_EXTRACT(team_data, '$.coach')]) AS coach_data  -- Extract the coach field from each team
		WHERE JSON_EXTRACT_SCALAR(team_data, '$.id') IS NOT NULL
)

SELECT
    coach_id,
    team_id,
    coach_name,
    coach_date_of_birth,
    coach_nationality,
    coach_contract_start,
    coach_contract_until,
    CURRENT_TIMESTAMP() AS loaded_date
FROM raw_coaches
WHERE row_num = 1
AND coach_id is not null
