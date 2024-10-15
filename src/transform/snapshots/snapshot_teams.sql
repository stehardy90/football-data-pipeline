{% snapshot snp_teams %}

  {{
    config(
      target_database='football-data-pipeline',
      target_schema='football_data_snapshot',
      unique_key='team_id',
      strategy='check',
      check_cols=['team_name', 'team_short_name', 'team_abbreviation', 'team_crest', 'team_address', 'team_website', 'team_founded_year', 'team_colours', 'team_venue'] 
    )
  }}

  SELECT
    team_id,
	team_name,
	team_short_name,
	team_abbreviation,
	team_crest,
	team_address,
	team_website,
	team_founded_year,
	team_colours,
	team_venue,
    CURRENT_TIMESTAMP() AS loaded_date
  FROM `football-data-pipeline.football_data_silver.stg_teams`

{% endsnapshot %}