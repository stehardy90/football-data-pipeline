{% snapshot snp_referees %}

  {{
    config(
      target_database='football-data-pipeline',
      target_schema='football_data_snapshot',
      unique_key='referee_id',
      strategy='check',
      check_cols=['referee_name', 'referee_type', 'referee_nationality'] 
    )
  }}

  SELECT
    referee_id,
	referee_name,
	referee_type,
	referee_nationality,
    CURRENT_TIMESTAMP() AS loaded_date
  FROM `football-data-pipeline.football_data_silver.stg_referees`

{% endsnapshot %}