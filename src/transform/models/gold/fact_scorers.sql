{{ config(
    materialized = 'incremental',
    unique_key = 'dbt_scd_id'  
) }}

WITH snapshot_data AS (
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
        {{ ref('snp_scorers') }} s
		
		LEFT JOIN {{ ref('dim_players') }} p
		ON s.player_id = p.player_id
		AND p.valid_to = '2099-12-31'
		
		LEFT JOIN {{ ref('dim_teams') }} t
		ON s.team_id = t.team_id
		AND t.valid_to = '2099-12-31'
		
		LEFT JOIN {{ ref('dim_competitions') }} c
		ON s.competition_id = c.competition_id
		AND c.valid_to = '2099-12-31'
)

SELECT *
FROM snapshot_data

{% if is_incremental() %}

WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} t
    WHERE snapshot_data.dbt_scd_id = t.dbt_scd_id
    AND COALESCE(snapshot_data.valid_from, '1900-01-01') = COALESCE(t.valid_from, '1900-01-01')
    AND COALESCE(snapshot_data.valid_to, '2099-12-31') = COALESCE(t.valid_to, '2099-12-31')
)

{% endif %}
