WITH raw_teams AS (

	SELECT * FROM {{ source('football_data','raw_football_teams')}}
	
)

,teams_stage AS (
    
	SELECT
        CAST(JSON_EXTRACT_SCALAR(team_data, '$.id') AS INT64) AS team_id,
        JSON_EXTRACT_SCALAR(team_data, '$.name') AS team_name,
        JSON_EXTRACT_SCALAR(team_data, '$.shortName') AS team_short_name,
        JSON_EXTRACT_SCALAR(team_data, '$.tla') AS team_abbreviation,
        JSON_EXTRACT_SCALAR(team_data, '$.crest') AS team_crest,
        JSON_EXTRACT_SCALAR(team_data, '$.address') AS team_address,
        JSON_EXTRACT_SCALAR(team_data, '$.website') AS team_website,
        CAST(JSON_EXTRACT_SCALAR(team_data, '$.founded') AS INT64) AS team_founded_year,
        JSON_EXTRACT_SCALAR(team_data, '$.clubColors') AS team_colours,
        JSON_EXTRACT_SCALAR(team_data, '$.venue') AS team_venue,
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

        CAST(JSON_EXTRACT_SCALAR(player_data, '$.id') AS INT64) AS player_id,
        JSON_EXTRACT_SCALAR(player_data, '$.name') AS player_name,
        COALESCE(JSON_EXTRACT_SCALAR(player_data, '$.position'), 'Unknown') AS player_position,
        CASE WHEN CAST(JSON_EXTRACT_SCALAR(player_data, '$.dateOfBirth') AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 10 YEAR) THEN NULL ELSE CAST(JSON_EXTRACT_SCALAR(player_data, '$.dateOfBirth') AS DATE) END AS player_date_of_birth,
        JSON_EXTRACT_SCALAR(player_data, '$.nationality') AS player_nationality,

        ROW_NUMBER() OVER (PARTITION BY JSON_EXTRACT_SCALAR(team_data, '$.id'), JSON_EXTRACT_SCALAR(player_data, '$.id') ORDER BY loaded_date DESC) AS row_num
    
	FROM 
		raw_teams,
		UNNEST(JSON_EXTRACT_ARRAY(raw_json, '$.teams')) AS team_data,  -- Unnest the teams array first
		UNNEST([JSON_EXTRACT(team_data, '$.coach')]) AS coach_data,  -- Extract the coach field from each team 
        UNNEST(JSON_EXTRACT_ARRAY(team_data, '$.squad')) AS player_data
	
	WHERE 
		JSON_EXTRACT_SCALAR(team_data, '$.id') IS NOT NULL
	
)

,final as (

	SELECT
		team_id,
		team_name,
		team_short_name,
		team_abbreviation,
		team_crest,
		team_address,
		team_website,
		team_founded_year,
		team_colours,
		team_venue,
        coach_id,
        coach_name,
        coach_date_of_birth,
        coach_nationality,
        coach_contract_start,
        coach_contract_until,
        player_id,
        player_name,
        player_position
        player_nationality,
        player_date_of_birth,
		CURRENT_TIMESTAMP() AS loaded_date
	FROM 
		teams_stage
	WHERE 
		row_num = 1
)

SELECT * FROM final
