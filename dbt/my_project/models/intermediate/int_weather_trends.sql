{{
    config(
        materialized='table',
    )
}}

select 
    city,
    date,

    AVG(avg_temperature) OVER (
        PARTITION BY city 
        ORDER BY date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as temp_7d_moving_avg,

    LAG(avg_temperature, 1) OVER (
        PARTITION BY city 
        ORDER BY date
    ) as prev_day_temp

from {{ ref('int_daily_weather_stats') }}

