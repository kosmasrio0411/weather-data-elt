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

select 
    city,
    date(weather_time_local) as date,
    
    round(avg(temperature), 2) as avg_temperature,
    min(temperature) as min_temperature,
    max(temperature) as max_temperature,
    
    round(avg(wind_speed), 2) as avg_wind_speed,
    max(wind_speed) as max_wind_speed,
    
    round(sum(precipitation), 2) as total_precipitation_mm,
    
    case 
        when sum(precipitation) > 0 then true 
        else false 
    end as is_rainy_day,

    max(epa_index) as max_epa_index

from {{ ref('stg_weather_data') }}
group by
    city,
    date(weather_time_local)