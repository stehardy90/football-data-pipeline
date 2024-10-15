SELECT
    t.team_name,
    c.competition_name,
    se.season_start_date,
    se.season_end_date,
    s.games_played,
    s.valid_from,
    s.valid_to,
    ROUND(SAFE_DIVIDE(sum(won),sum(games_played))*100,2) AS win_rate,
    ROUND(SAFE_DIVIDE(sum(drew),sum(games_played))*100,2) AS draw_rate,
    ROUND(SAFE_DIVIDE(sum(lost),sum(games_played))*100,2) AS loss_rate,
    ROUND(SAFE_DIVIDE(sum(points),sum(games_played)),1) AS points_per_game,
    ROUND(SAFE_DIVIDE(sum(goals_for),sum(games_played)),1) AS scored_per_game,
    ROUND(SAFE_DIVIDE(sum(goals_against),sum(games_played)),1) AS conceded_per_game,
    ROUND(SAFE_DIVIDE(sum(goal_difference),sum(games_played)),1) AS average_goal_difference,    
FROM
    football-data-pipeline.football_data_aggregate.fact_standings s

    JOIN football-data-pipeline.football_data_aggregate.dim_teams t
    ON s.team_id = t.team_id

    JOIN football-data-pipeline.football_data_aggregate.dim_competitions c
    ON c.competition_id = s.competition_id

    JOIN football-data-pipeline.football_data_aggregate.dim_seasons se
    ON se.season_id = s.season_id

GROUP BY
    c.competition_name,
    se.season_start_date,
    se.season_end_date,
    t.team_name,
    s.games_played,
    s.valid_from,
    s.valid_to




