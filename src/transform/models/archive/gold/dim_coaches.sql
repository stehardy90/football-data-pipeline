{{ config(
    materialized = 'incremental',
    unique_key = 'dbt_scd_id' 
) }}

WITH coaches_snapshot AS (

	SELECT * FROM {{ ref('snp_coaches') }}

)

,final AS (

    SELECT
        c.coach_id,
        c.team_id,
        c.coach_name,
        c.coach_date_of_birth,
        c.coach_nationality,
        c.coach_contract_start,
        CASE 
          WHEN DATE(c.dbt_valid_to) <> '2099-12-31' THEN DATE(c.dbt_valid_to)
          ELSE c.coach_contract_until END AS coach_contract_end,
        c.dbt_valid_from AS valid_from,
        IFNULL(c.dbt_valid_to, '2099-12-31') AS valid_to,
        c.dbt_updated_at AS loaded_date,  
        c.dbt_scd_id,  
        CASE 
            WHEN IFNULL(c.dbt_valid_to, '2099-12-31') = '2099-12-31' THEN 1
            ELSE 0
        END AS current_flag  
   
   FROM
        coaches_snapshot c
		
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
