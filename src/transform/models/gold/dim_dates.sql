SELECT 
    CAST(day as DATE) AS date,
    EXTRACT(YEAR FROM day) AS year,
    EXTRACT(MONTH FROM day) AS month,
    EXTRACT(DAY FROM day) AS day_of_month,
    EXTRACT(WEEK FROM day) AS week,
    EXTRACT(DAYOFWEEK FROM day) AS day_of_week,
    EXTRACT(QUARTER FROM day) AS quarter,
    DATE_TRUNC(day, WEEK(MONDAY)) AS week_start_date,  
    DATE_TRUNC(day, MONTH) AS month_start_date      
FROM 
    UNNEST(GENERATE_DATE_ARRAY('2020-01-01', DATE_SUB(DATE_ADD(DATE_TRUNC(CURRENT_DATE(), YEAR), INTERVAL 4 YEAR), INTERVAL 1 DAY), INTERVAL 1 DAY)) AS day
