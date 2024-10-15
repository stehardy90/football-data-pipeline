{% snapshot snp_players %}

  {{
    config(
      target_database='football-data-pipeline',
      target_schema='football_data_snapshot',
      unique_key='player_id',
      strategy='check',
      check_cols=['team_id', 'player_name', 'player_position', 'player_date_of_birth', 'player_nationality'] 
    )
  }}

  SELECT
    player_id,
	team_id,
	player_name,
	player_position,
	player_date_of_birth,
	player_nationality,
    CURRENT_TIMESTAMP() AS loaded_date
  FROM `football-data-pipeline.football_data_silver.stg_players`

{% endsnapshot %}