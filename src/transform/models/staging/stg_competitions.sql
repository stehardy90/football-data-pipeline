WITH raw_competitions AS (

	SELECT * FROM {{ source('football_data','raw_football_competitions')}}
	
)

,competitions_stage as (

    SELECT
        CAST(JSON_EXTRACT_SCALAR(raw_json, '$.id') AS INT64) AS competition_id,
        JSON_EXTRACT_SCALAR(raw_json, '$.name') AS competition_name,
        JSON_EXTRACT_SCALAR(raw_json, '$.code') AS competition_code,
        JSON_EXTRACT_SCALAR(raw_json, '$.type') AS competition_type,
        JSON_EXTRACT_SCALAR(raw_json, '$.emblem') AS competition_emblem,
        JSON_EXTRACT_SCALAR(raw_json, '$.area.code') AS area_code,
        JSON_EXTRACT_SCALAR(raw_json, '$.area.name') AS area_name,
        JSON_EXTRACT_SCALAR(raw_json, '$.area.flag') AS area_flag,
        CAST(JSON_EXTRACT_SCALAR(season_data, '$.id') AS INT64) AS season_id,
        CAST(JSON_EXTRACT_SCALAR(season_data, '$.startDate') AS DATE) AS season_start_date,
        CAST(JSON_EXTRACT_SCALAR(season_data, '$.endDate') AS DATE) AS season_end_date,
        CAST(JSON_EXTRACT_SCALAR(season_data, '$.currentMatchday') AS INT64) AS current_matchday,
        ROW_NUMBER() OVER (PARTITION BY JSON_EXTRACT_SCALAR(raw_json, '$.id'), JSON_EXTRACT_SCALAR(season_data, '$.id') ORDER BY loaded_date DESC) AS row_num
    FROM 
		raw_competitions,  
		UNNEST(JSON_EXTRACT_ARRAY(raw_json, '$.seasons')) AS season_data  
	
	WHERE 
		JSON_EXTRACT_SCALAR(raw_json, '$.id') IS NOT NULL
		
)

,final as (

	SELECT
		competition_id,
		competition_name,
		competition_code,
		competition_type,
		competition_emblem,
		area_code,
        area_name,
        area_flag,
        season_id,
        season_start_date,
        season_end_date,
        current_matchday,
		CURRENT_TIMESTAMP() AS loaded_date
	FROM 
		competitions_stage
	WHERE 
		row_num = 1

)

SELECT * FROM final