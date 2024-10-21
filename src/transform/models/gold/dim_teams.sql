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
