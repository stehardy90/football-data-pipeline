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

  SELECT * FROM {{ ref('stg_referees') }}

{% endsnapshot %}