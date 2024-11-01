{{ config(
    materialized = 'incremental',
    unique_key = 'match_id'
) }}

WITH matches_stage AS (

	SELECT * FROM {{ ref('stg_matches') }}

)

,referees_snapshot AS (

	SELECT * FROM {{ ref('snp_referees') }}

)


,final AS (
    
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
        matches_stage m
	
	LEFT JOIN referees_snapshot r
	ON r.referee_id = m.referee_id
	and r.dbt_valid_to is null
	
)

SELECT * FROM final


{% if is_incremental() %}

WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} t
    WHERE final.match_id = t.match_id
    AND COALESCE(final.match_status, '') = COALESCE(t.match_status, '')  
)

{% endif %}
