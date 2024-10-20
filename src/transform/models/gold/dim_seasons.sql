{{ config(
    materialized = 'incremental',
    unique_key = 'dbt_scd_id'  
) }}

WITH snapshot_data AS (
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
        {{ ref('snp_seasons') }}  
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
