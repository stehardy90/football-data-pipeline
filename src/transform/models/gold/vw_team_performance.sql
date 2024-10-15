{{ config(
    materialized='view'
) }}

SELECT
    t.team_name,
    c.competition_name,
    se.season_start_date,
    se.season_end_date,
    s.games_played,
    s.valid_from,
    s.valid_to,
    ROUND(SAFE_DIVIDE(SUM(won), SUM(games_played)) * 100, 2) AS win_rate,
    ROUND(SAFE_DIVIDE(SUM(drew), SUM(games_played)) * 100, 2) AS draw_rate,
    ROUND(SAFE_DIVIDE(SUM(lost), SUM(games_played)) * 100, 2) AS loss_rate,
    ROUND(SAFE_DIVIDE(SUM(points), SUM(games_played)), 1) AS points_per_game,
    ROUND(SAFE_DIVIDE(SUM(goals_for), SUM(games_played)), 1) AS scored_per_game,
    ROUND(SAFE_DIVIDE(SUM(goals_against), SUM(games_played)), 1) AS conceded_per_game,
    ROUND(SAFE_DIVIDE(SUM(goal_difference), SUM(games_played)), 1) AS average_goal_difference
FROM
    {{ ref('fact_standings') }} s  -- Use ref() to reference the table
JOIN
    {{ ref('dim_teams') }} t ON s.team_id = t.team_id
JOIN
    {{ ref('dim_competitions') }} c ON c.competition_id = s.competition_id
JOIN
    {{ ref('dim_seasons') }} se ON se.season_id = s.season_id
GROUP BY
    c.competition_name,
    se.season_start_date,
    se.season_end_date,
    t.team_name,
    s.games_played,
    s.valid_from,
    s.valid_to