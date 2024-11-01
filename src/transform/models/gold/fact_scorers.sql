{{ config(
    materialized = 'incremental',
    unique_key = 'dbt_scd_id'  
) }}


WITH scorers_snapshot AS (

	SELECT * FROM {{ ref('snp_scorers') }}

)

,players_snapshot AS (

	SELECT * FROM {{ ref('dim_players') }}

)

,teams_snapshot AS (

	SELECT * FROM {{ ref('dim_teams') }}

)

,competitions_snapshot AS (

	SELECT * FROM {{ ref('dim_competitions') }}

)


,final AS (
    SELECT
        s.player_id,
		p.player_name,
        s.team_id,
		t.team_name,
        s.played_matches,
        s.goals,
        s.assists,
        s.penalties,
        s.competition_id,
		c.competition_name,
        s.season_id,
        s.current_matchday,
        s.dbt_valid_from AS valid_from, 
        IFNULL(s.dbt_valid_to, '2099-12-31') AS valid_to, 
        s.dbt_updated_at AS loaded_date,  
        s.dbt_scd_id,  
        CASE 
            WHEN IFNULL(s.dbt_valid_to, '2099-12-31') = '2099-12-31' THEN 1
            ELSE 0
        END AS current_flag  
    FROM
        scorers_snapshot s
		
		LEFT JOIN players_snapshot p
		ON s.player_id = p.player_id
		AND p.valid_to = '2099-12-31'
		
		LEFT JOIN teams_snapshot t
		ON s.team_id = t.team_id
		AND t.valid_to = '2099-12-31'
		
		LEFT JOIN competitions_snapshot c
		ON s.competition_id = c.competition_id
		AND c.valid_to = '2099-12-31'
		
)

SELECT * FROM final

{% if is_incremental() %}

WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} t
    WHERE final.dbt_scd_id = t.dbt_scd_id
    AND COALESCE(final.valid_from, '1900-01-01') = COALESCE(t.valid_from, '1900-01-01')
    AND COALESCE(final.valid_to, '2099-12-31') = COALESCE(t.valid_to, '2099-12-31')
)

{% endif %}
