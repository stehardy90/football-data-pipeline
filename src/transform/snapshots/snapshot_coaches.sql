{% snapshot snp_coaches %}

  {{
    config(
      target_database='football-data-pipeline',
      target_schema='football_data_snapshot',
      unique_key='coach_id',
      strategy='check',
      check_cols=['team_id', 'coach_name','coach_date_of_birth','coach_nationality','coach_contract_start','coach_contract_until'] 
    )
  }}

  SELECT
  coach_id,
  team_id,
  coach_name,
	coach_date_of_birth,
	coach_nationality,
	coach_contract_start,
	coach_contract_until,
    CURRENT_TIMESTAMP() AS loaded_date
  FROM `football-data-pipeline.football_data_silver.stg_coaches`

{% endsnapshot %}