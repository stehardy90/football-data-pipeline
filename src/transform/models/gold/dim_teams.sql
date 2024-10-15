select 
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
    dbt_valid_from as valid_from,
    ifnull(dbt_valid_to,'2099-12-31') as valid_to,    
    CURRENT_TIMESTAMP() AS loaded_date   
from 
    football-data-pipeline.football_data_snapshot.snp_teams



