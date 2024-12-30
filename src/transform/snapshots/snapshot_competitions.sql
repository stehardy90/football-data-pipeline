{% snapshot snp_competitions %}

  {{
    config(
      target_database='football-data-pipeline',
      target_schema='football_data_snapshot',
      unique_key="CONCAT(CAST(competition_id AS STRING), '-', CAST(season_id AS STRING))",
      strategy='check',
      check_cols=['competition_name', 'competition_code', 'competition_type', 'competition_emblem', 'area_code', 'area_name', 'area_flag', 'season_start_date', 'season_end_date', 'current_matchday'] 
    )
  }}

  SELECT * FROM {{ ref('stg_competitions') }}

{% endsnapshot %}