{% snapshot snp_competitions %}

  {{
    config(
      target_database='football-data-pipeline',
      target_schema='football_data_snapshot',
      unique_key='competition_id',
      strategy='check',
      check_cols=['competition_name', 'competition_code', 'competition_type', 'competition_emblem', 'area_code'] 
    )
  }}

  SELECT
    competition_id,
    competition_name,
    competition_code,
	competition_type,
	competition_emblem,
	area_code,
    CURRENT_TIMESTAMP() AS loaded_date
  FROM `football-data-pipeline.football_data_silver.stg_competitions`

{% endsnapshot %}