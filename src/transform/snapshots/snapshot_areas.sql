{% snapshot snp_areas %}

  {{
    config(
      target_database='football-data-pipeline',
      target_schema='football_data_snapshot',
      unique_key='area_code',
      strategy='check',
      check_cols=['area_name', 'area_flag'] 
    )
  }}

  SELECT
    area_code,
    area_name,
    area_flag,
    CURRENT_TIMESTAMP() AS loaded_date
  FROM `football-data-pipeline.football_data_silver.stg_areas`

{% endsnapshot %}