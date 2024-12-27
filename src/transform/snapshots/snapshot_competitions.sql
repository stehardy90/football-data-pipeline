{% snapshot snp_competitions %}

  {{
    config(
      target_database='football-data-pipeline',
      target_schema='football_data_snapshot',
      unique_key='competition_id',
      strategy='check',
      check_cols=['competition_name', 'competition_code', 'competition_type', 'competition_emblem', 'area_code', 'area_name', 'area_flag', 'season_id', 'season_start_date', 'season_end_date', 'current_matchday'] 
    )
  }}

  SELECT * FROM {{ ref('stg_competitions') }}

{% endsnapshot %}