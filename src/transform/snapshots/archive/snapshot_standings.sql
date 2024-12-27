{% snapshot snp_standings %}

  {{
    config(
      target_database='football-data-pipeline',
      target_schema='football_data_snapshot',
      unique_key='composite_unique_key',
      strategy='check',
      check_cols=['team_id', 'played_games', 'won', 'draw', 'lost', 'points', 'goals_for', 'goals_against', 'goal_difference', 'competition_id', 'season_id']
	)
  }}

  SELECT *,
	CONCAT(CAST(competition_id AS STRING), '-', CAST(season_id AS STRING), '-', CAST(league_position AS STRING)) AS composite_unique_key
  FROM {{ ref('stg_standings') }}

{% endsnapshot %}