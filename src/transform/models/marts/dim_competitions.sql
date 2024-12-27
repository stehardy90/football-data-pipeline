{{ config(
    materialized = 'incremental',
    unique_key = 'dbt_scd_id'  
) }}

WITH competitions_snapshot AS (

	SELECT * FROM {{ ref('snp_competitions') }}

)

,final AS (
    
	SELECT
        c.competition_id,
        c.competition_name,
        c.competition_code,
        c.competition_type,
        c.competition_emblem,
        c.area_code,
        c.area_name,
        c.area_flag,
        c.season_id,
        c.season_start_date,
        c.season_end_date,
        current_matchday,
        c.dbt_valid_from AS valid_from,  
        IFNULL(c.dbt_valid_to, '2099-12-31') AS valid_to,  
        c.dbt_updated_at AS loaded_date,  
        c.dbt_scd_id,  
        CASE 
            WHEN IFNULL(c.dbt_valid_to, '2099-12-31') = '2099-12-31' THEN 1
            ELSE 0
        END AS current_flag 
    
	FROM
        competitions_snapshot c
    
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
