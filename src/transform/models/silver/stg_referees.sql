WITH raw_referees AS (
    SELECT
        CAST(JSON_EXTRACT_SCALAR(ref_data, '$.id') AS INT64) AS referee_id,
        JSON_EXTRACT_SCALAR(ref_data, '$.name') AS referee_name,
        JSON_EXTRACT_SCALAR(ref_data, '$.type') AS referee_type,
		COALESCE(JSON_EXTRACT_SCALAR(ref_data, '$.nationality'), 'Unknown') AS referee_nationality,
        ROW_NUMBER() OVER (PARTITION BY JSON_EXTRACT_SCALAR(ref_data, '$.id') ORDER BY loaded_date DESC) AS row_num
    FROM `{{ var('bigquery_dataset') }}.raw_football_matches`,
    UNNEST(JSON_EXTRACT_ARRAY(raw_json, '$.matches')) AS match_data,  -- Unnest the matches array
    UNNEST(JSON_EXTRACT_ARRAY(match_data, '$.referees')) AS ref_data  -- Unnest the referees array inside each match
	WHERE JSON_EXTRACT_SCALAR(ref_data, '$.id') IS NOT NULL
)

SELECT
    referee_id,
    referee_name,
    referee_type,
    referee_nationality,
    CURRENT_TIMESTAMP() AS loaded_date
FROM raw_referees
WHERE row_num = 1
