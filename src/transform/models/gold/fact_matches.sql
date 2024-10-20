{{ config(
    materialized = 'incremental',
    unique_key = 'match_id'
) }}

WITH snapshot_data AS (
    SELECT
        m.match_id,
        m.home_team_id,
        m.away_team_id,
        m.score_home,
        m.score_away,
        m.half_time_home,
        m.half_time_away,
        m.match_status,
        m.match_day,
        m.competition_id,
        m.season_id,
        m.referee_id,
		r.referee_name,
		r.referee_nationality,
        CURRENT_TIMESTAMP() AS loaded_date  
    FROM 
        {{ ref('stg_matches') }} m
	LEFT JOIN {{ ref('snp_referees') }} r
	ON r.referee_id = m.referee_id
	and r.dbt_valid_to is null
)

SELECT *
FROM snapshot_data

{% if is_incremental() %}

WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} t
    WHERE snapshot_data.match_id = t.match_id
    AND COALESCE(snapshot_data.match_status, '') = COALESCE(t.match_status, '')  
)

{% endif %}
