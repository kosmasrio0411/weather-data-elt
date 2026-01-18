{{
    config(
        materialized='table',
        unique_key='id'
    )
}}

with source as (
    select *
    from {{ source('dev', 'raw_weather_data') }}
),

de_dup as (
    select 
        *,
        row_number() over(partition by time order by inserted_at desc) as rn
    from source
)

select 
    id,
    city,
    time as weather_time_local,

    TIMESTAMP_ADD(inserted_at, INTERVAL CAST(CAST(utc_offset AS FLOAT64) AS INT64) HOUR) as inserted_at_utc,

    temperature,
    weather_descriptions,
    wind_speed,
    wind_degree,   
    pressure,    
    precipitation, 
    humidity,   
    cloudcover,   

    SAFE_CAST(air_quality.co AS FLOAT64) as aq_co,
    SAFE_CAST(air_quality.no2 AS FLOAT64) as aq_no2,
    SAFE_CAST(air_quality.o3 AS FLOAT64) as aq_o3,
    SAFE_CAST(air_quality.pm2_5 AS FLOAT64) as aq_pm2_5,
    SAFE_CAST(air_quality.pm10 AS FLOAT64) as aq_pm10,

    CAST(CAST(air_quality.`us-epa-index` AS FLOAT64) AS INT64) as epa_index

from de_dup
where rn = 1