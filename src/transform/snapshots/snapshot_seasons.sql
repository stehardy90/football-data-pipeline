{% snapshot snp_seasons %}

  {{
    config(
      target_database='football-data-pipeline',
      target_schema='football_data_snapshot',
      unique_key='season_id',
      strategy='check',
      check_cols=['season_start_date', 'season_end_date', 'current_matchday', 'competition_id'] 
    )
  }}

  SELECT
    season_id,
	season_start_date,
	season_end_date,
	current_matchday,
	competition_id,
    CURRENT_TIMESTAMP() AS loaded_date
  FROM `football-data-pipeline.football_data_silver.stg_seasons`

{% endsnapshot %}