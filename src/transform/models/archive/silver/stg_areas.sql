WITH raw_competitions AS (

	SELECT * FROM {{ source('football_data_pipeline','raw_football_competitions') }}
	
)

,areas_stage AS (
    
	SELECT DISTINCT
        JSON_EXTRACT_SCALAR(raw_json, '$.area.code') AS area_code,
        JSON_EXTRACT_SCALAR(raw_json, '$.area.name') AS area_name,
        JSON_EXTRACT_SCALAR(raw_json, '$.area.flag') AS area_flag,
        ROW_NUMBER() OVER (PARTITION BY JSON_EXTRACT_SCALAR(raw_json, '$.area.code') ORDER BY loaded_date DESC) AS row_num
    
	FROM 
		raw_competitions
	
	WHERE 
		JSON_EXTRACT_SCALAR(raw_json, '$.area.code') IS NOT NULL
		
)

,final AS (
	
	SELECT
		area_code,
		area_name,
		area_flag,
		CURRENT_TIMESTAMP() AS loaded_date
	
	FROM 
		areas_stage
	
	WHERE 
		row_num = 1

)


SELECT * FROM final