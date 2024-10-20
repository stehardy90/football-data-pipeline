{{ config(
    materialized = 'incremental',
    unique_key = 'dbt_scd_id' 
) }}

WITH snapshot_data AS (
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
		c.coach_id,
		c.coach_name,
		c.coach_date_of_birth,
		c.coach_nationality,
		c.coach_contract_start,
		c.coach_contract_until,
        t.dbt_valid_from AS valid_from,
        IFNULL(t.dbt_valid_to, '2099-12-31') AS valid_to,
        t.dbt_updated_at AS loaded_date,  
        t.dbt_scd_id,  
        CASE 
            WHEN IFNULL(t.dbt_valid_to, '2099-12-31') = '2099-12-31' THEN 1
            ELSE 0
        END AS current_flag  
    FROM
        {{ ref('snp_teams') }} t
	LEFT JOIN {{ ref('snp_coaches') }} c
	ON c.team_id = t.team_id
    AND t.dbt_valid_from between c.dbt_valid_from and ifnull(c.dbt_valid_to,'2099-12-31')  
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
