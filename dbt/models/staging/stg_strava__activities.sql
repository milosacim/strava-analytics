{{
    config(
        alias='stg_activities',
        materialized = 'incremental',
        unique_key='activity_id',
        on_schema_change='fail',
        cluster_by = ['start_date'],
        incremental_strategy='merge'
        
    )
}}

with raw_act as (
    select * from {{ source('raw_activities', 'ext_activities_raw') }}

),

final as (
    select
        id                                    as activity_id,
        athlete.id                            as athlete_id,
        name                                  as activity_name,
        sport_type,
        cast(start_date as date)              as start_date,
        cast(distance / 1000 as numeric)      as distance_km,
        cast(elapsed_time / 60 as numeric)    as elapsed_time_mins,
        cast(total_elevation_gain as numeric) as total_elevation_gain,
        cast(average_speed * 3.6 as numeric)  as average_speed_kmh,
        cast(max_speed * 3.6 as numeric)      as max_speed_kmh,
        cast(average_watts as numeric)        as average_watts,
        cast(average_heartrate as numeric)    as average_heartrate,
        cast(max_heartrate as numeric)        as max_heartrate,
        cast(suffer_score as numeric)         as suffer_score
    from raw_act
)

select * from final
