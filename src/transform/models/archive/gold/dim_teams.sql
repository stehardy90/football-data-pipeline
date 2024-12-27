{{ config(
    materialized = 'incremental',
    unique_key = 'dbt_scd_id' 
) }}

WITH teams_snapshot AS (

	SELECT * FROM {{ ref('snp_teams') }}

)

final AS (

    SELECT
        t.team_id,
        t.team_name,
        t.team_short_name,
        t.team_abbreviation,
        t.team_crest,
        t.team_address,
        t.team_website,
        t.team_founded_year,
        t.team_colours,
        t.team_venue,
        t.dbt_valid_from AS valid_from,
        IFNULL(t.dbt_valid_to, '2099-12-31') AS valid_to,
        t.dbt_updated_at AS loaded_date,  
        t.dbt_scd_id,  
        CASE 
            WHEN IFNULL(t.dbt_valid_to, '2099-12-31') = '2099-12-31' THEN 1
            ELSE 0
        END AS current_flag  
    
	FROM
        teams_snapshot t
		
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
