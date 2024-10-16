{% macro test_date_of_birth_more_than_10_years_ago(model) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} >= DATE_SUB(CURRENT_DATE(), INTERVAL 10 YEAR)
{% endmacro %}

{% macro test_contract_start_before_end(model) %}
    SELECT *
    FROM {{ model }}
    WHERE coach_contract_start >= coach_contract_until
{% endmacro %}

{% macro test_half_time_score_valid(model) %}
    SELECT *
    FROM {{ model }}
    WHERE half_time_away > score_away
       OR half_time_home > score_home
{% endmacro %}

{% macro test_season_start_before_end(model) %}
    SELECT *
    FROM {{ model }}
    WHERE season_start_date >= season_end_date
{% endmacro %}

{% macro test_goal_difference_and_points_valid(model) %}
    SELECT *
    FROM {{ model }}
    WHERE 
        goal_difference != (goals_for - goals_against)
        OR points > (games_played * 3)
        OR won > games_played
        OR lost > games_played
        OR drew > games_played
{% endmacro %}

{% macro test_custom_accepted_values(column_name, values, model=None) %}
SELECT *
FROM {{ model }}
WHERE {{ column_name }} NOT IN ({{ values | join(', ') }})
{% endmacro %}

{% macro test_custom_range(column_name, min_value, max_value, model=None) %}
SELECT *
FROM {{ model }}
WHERE {{ column_name }} < {{ min_value }}
   OR {{ column_name }} > {{ max_value }}
{% endmacro %}
