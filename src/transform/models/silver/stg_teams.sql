WITH raw_teams AS (

	SELECT * FROM {{ source('football_data_pipeline','raw_football_teams') }}
	
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
        ROW_NUMBER() OVER (PARTITION BY JSON_EXTRACT_SCALAR(team_data, '$.id') ORDER BY loaded_date DESC) AS row_num
    
	FROM 
		raw_teams,
		UNNEST(JSON_EXTRACT_ARRAY(raw_json, '$.teams')) AS team_data  -- Unnest the teams array
	
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
		CURRENT_TIMESTAMP() AS loaded_date
	FROM 
		raw_teams
	WHERE 
		row_num = 1
)

SELECT * FROM final
