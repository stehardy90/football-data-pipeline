with raw_competitions as (
    select
        CAST(JSON_EXTRACT_SCALAR(raw_json, '$.id') AS INT64) AS competition_id,
        JSON_EXTRACT_SCALAR(raw_json, '$.name') AS competition_name,
        JSON_EXTRACT_SCALAR(raw_json, '$.code') AS competition_code,
        JSON_EXTRACT_SCALAR(raw_json, '$.type') AS competition_type,
        JSON_EXTRACT_SCALAR(raw_json, '$.emblem') AS competition_emblem,
        JSON_EXTRACT_SCALAR(raw_json, '$.area.code') AS area_code,
        row_number() over (partition by JSON_EXTRACT_SCALAR(raw_json, '$.id') order by loaded_date desc) as row_num
    from `{{ var('bigquery_dataset') }}.raw_football_competitions`
	WHERE JSON_EXTRACT_SCALAR(raw_json, '$.id') IS NOT NULL
)

select
    competition_id,
    competition_name,
    competition_code,
    competition_type,
    competition_emblem,
    area_code,
    CURRENT_TIMESTAMP() AS loaded_date
from raw_competitions
where row_num = 1
