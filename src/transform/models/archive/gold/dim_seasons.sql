{{ config(
    materialized = 'incremental',
    unique_key = 'dbt_scd_id'  
) }}

WITH seasons_snapshot AS (

	SELECT * FROM {{ ref('snp_seasons') }}

)

,final AS (
    
	SELECT
        season_id,
        season_start_date,
        season_end_date,
        current_matchday,
        competition_id,
        dbt_valid_from AS valid_from,  
        IFNULL(dbt_valid_to, '2099-12-31') AS valid_to,  
        dbt_updated_at AS loaded_date,  
        dbt_scd_id,  
        CASE 
            WHEN IFNULL(dbt_valid_to, '2099-12-31') = '2099-12-31' THEN 1
            ELSE 0
        END AS current_flag  
    
	FROM
        seasons_snapshot 
		
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
