with raw_areas as (
    select distinct
        JSON_EXTRACT_SCALAR(raw_json, '$.area.code') AS area_code,
        JSON_EXTRACT_SCALAR(raw_json, '$.area.name') AS area_name,
        JSON_EXTRACT_SCALAR(raw_json, '$.area.flag') AS area_flag,
        row_number() over (partition by JSON_EXTRACT_SCALAR(raw_json, '$.area.code') order by loaded_date desc) as row_num
    from `{{ var('bigquery_dataset') }}.raw_football_competitions`
	WHERE JSON_EXTRACT_SCALAR(raw_json, '$.area.code') IS NOT NULL
)

select
	area_code,
	area_name,
	area_flag,
	CURRENT_TIMESTAMP() AS loaded_date
from raw_areas
where row_num = 1