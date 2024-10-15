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

  SELECT * FROM {{ ref('stg_players') }}

{% endsnapshot %}