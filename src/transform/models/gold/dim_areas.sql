SELECT
    area_code,
    area_name,
    area_flag,
    dbt_valid_from as valid_from,
    ifnull(dbt_valid_to,'2099-12-31') as valid_to,    
    CURRENT_TIMESTAMP() AS loaded_date
FROM
    {{ ref('snp_areas') }}