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

  SELECT * FROM {{ ref('stg_areas') }}

{% endsnapshot %}