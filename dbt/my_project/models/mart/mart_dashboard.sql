{{
    config(
        materialized='table',
        partition_by={
            "field": "date",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

with stats as (
    select * from {{ ref('int_daily_weather_stats') }}
),

trends as (
    select * from {{ ref('int_weather_trends') }}
)

select 
    -- 1. Main metrics
    stats.city,
    stats.date,
    stats.avg_temperature,
    stats.min_temperature,
    stats.max_temperature,
    stats.avg_wind_speed,
    stats.max_wind_speed,
    stats.total_precipitation_mm,
    stats.is_rainy_day,
    stats.max_epa_index,

    -- 2. Tren
    trends.temp_7d_moving_avg,
    trends.prev_day_temp,
    (stats.avg_temperature - trends.prev_day_temp) as temp_change_from_yesterday,

    -- 3. Label Air Quality
    CASE 
        WHEN stats.max_epa_index = 1 THEN 'Good'
        WHEN stats.max_epa_index = 2 THEN 'Moderate'
        WHEN stats.max_epa_index = 3 THEN 'Unhealthy for Sensitive Groups'
        WHEN stats.max_epa_index = 4 THEN 'Unhealthy'
        WHEN stats.max_epa_index = 5 THEN 'Very Unhealthy'
        WHEN stats.max_epa_index >= 6 THEN 'Hazardous'
        ELSE 'Unknown'
    END as air_quality_label,

    -- 4. Rekomendasi Aktivitas
    CASE 
        WHEN stats.total_precipitation_mm = 0 
             AND stats.max_epa_index <= 2 
             AND stats.avg_temperature < 30 
        THEN true ELSE false 
    END as is_good_for_running,
    CASE 
        WHEN stats.total_precipitation_mm > 0 
        THEN true ELSE false 
    END as need_umbrella,
    CASE 
        WHEN stats.total_precipitation_mm = 0 
             AND (stats.avg_temperature > 28 OR stats.avg_wind_speed > 15)
        THEN true ELSE false 
    END as fast_drying_laundry

from stats
left join trends 
    on stats.city = trends.city 
    and stats.date = trends.date