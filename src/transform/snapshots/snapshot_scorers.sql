{% snapshot snp_scorers %}
  {{
    config(
      target_database='football-data-pipeline',
      target_schema='football_data_snapshot',  
      unique_key='composite_unique_key',  
      strategy='check',
      check_cols=['played_matches', 'goals', 'assists', 'penalties', 'current_matchday']  
    )
  }}

  SELECT
    player_id,
    team_id,
    played_matches,
    goals,
    assists,
    penalties,
    competition_id,
    season_id,
    current_matchday,
    CURRENT_TIMESTAMP() AS snapshot_loaded_date,
	CONCAT(CAST(player_id AS STRING), '-', CAST(team_id AS STRING), '-', CAST(competition_id AS STRING), '-', CAST(season_id AS STRING)) AS composite_unique_key
  FROM football-data-pipeline.football_data_silver.stg_scorers

{% endsnapshot %}
