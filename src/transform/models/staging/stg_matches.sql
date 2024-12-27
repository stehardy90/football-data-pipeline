WITH raw_matches AS (

	SELECT * FROM {{ source('football_data','raw_football_matches')}}
	
)

,matches_stage AS (
    
	SELECT
        CAST(JSON_EXTRACT_SCALAR(match_data, '$.id') AS INT64) AS match_id,
        CAST(JSON_EXTRACT_SCALAR(match_data, '$.homeTeam.id') AS INT64) AS home_team_id,
        CAST(JSON_EXTRACT_SCALAR(match_data, '$.awayTeam.id') AS INT64) AS away_team_id,
        CAST(COALESCE(JSON_EXTRACT_SCALAR(match_data, '$.score.fullTime.home'), '0') AS INT64) AS score_home,
        CAST(COALESCE(JSON_EXTRACT_SCALAR(match_data, '$.score.fullTime.away'), '0') AS INT64) AS score_away,
        CAST(COALESCE(JSON_EXTRACT_SCALAR(match_data, '$.score.halfTime.home'), '0') AS INT64) AS half_time_home,
        CAST(COALESCE(JSON_EXTRACT_SCALAR(match_data, '$.score.halfTime.away'), '0') AS INT64) AS half_time_away,
        JSON_EXTRACT_SCALAR(match_data, '$.status') AS match_status,
        CAST(JSON_EXTRACT_SCALAR(match_data, '$.utcDate') AS TIMESTAMP) AS match_day,
        CAST(JSON_EXTRACT_SCALAR(raw_json, '$.competition.id') AS INT64) AS competition_id,
        CAST(JSON_EXTRACT_SCALAR(match_data, '$.season.id') AS INT64) AS season_id,
        CAST(JSON_EXTRACT_SCALAR(ref_data, '$.id') AS INT64) AS referee_id,
        JSON_EXTRACT_SCALAR(ref_data, '$.name') AS referee_name,
        JSON_EXTRACT_SCALAR(ref_data, '$.type') AS referee_type,
		COALESCE(JSON_EXTRACT_SCALAR(ref_data, '$.nationality'), 'Unknown') AS referee_nationality,
        ROW_NUMBER() OVER (PARTITION BY JSON_EXTRACT_SCALAR(match_data, '$.id') ORDER BY loaded_date DESC) AS row_num
    FROM 
		raw_matches,
		UNNEST(JSON_EXTRACT_ARRAY(raw_json, '$.matches')) AS match_data,  
		UNNEST(JSON_EXTRACT_ARRAY(match_data, '$.referees')) AS ref_data  
	
	WHERE 	
		JSON_EXTRACT_SCALAR(match_data, '$.id') IS NOT NULL
)

,final as (

	SELECT
		match_id,
		home_team_id,
		away_team_id,
		score_home,
		score_away,
		half_time_home,
		half_time_away,
		match_status,
		match_day,
		competition_id,
		season_id,
		referee_id,
        referee_name,
        referee_type,
        referee_nationality,
		CURRENT_TIMESTAMP() AS loaded_date
	FROM 
		matches_stage
	WHERE 
		row_num = 1

)

SELECT * FROM final

