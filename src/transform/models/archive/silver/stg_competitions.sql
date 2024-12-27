WITH raw_competitions AS (

	SELECT * FROM {{ source('football_data_pipeline','raw_football_competitions') }}
	
)

,competitions_stage as (

    SELECT
        CAST(JSON_EXTRACT_SCALAR(raw_json, '$.id') AS INT64) AS competition_id,
        JSON_EXTRACT_SCALAR(raw_json, '$.name') AS competition_name,
        JSON_EXTRACT_SCALAR(raw_json, '$.code') AS competition_code,
        JSON_EXTRACT_SCALAR(raw_json, '$.type') AS competition_type,
        JSON_EXTRACT_SCALAR(raw_json, '$.emblem') AS competition_emblem,
        JSON_EXTRACT_SCALAR(raw_json, '$.area.code') AS area_code,
        ROW_NUMBER() OVER (PARTITION BY JSON_EXTRACT_SCALAR(raw_json, '$.id') ORDER BY loaded_date DESC) AS row_num
    FROM 
		raw_competitions
	
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
		CURRENT_TIMESTAMP() AS loaded_date
	FROM 
		competitions_stage
	WHERE 
		row_num = 1

)

SELECT * FROM final