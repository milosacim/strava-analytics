-- Silver: one row per Strava activity. Incremental MERGE on activity_id; scans last 15 days of source.
{{
    config(
        alias='stg_activities',
        materialized = 'incremental',
        unique_key='activity_id',
        on_schema_change='fail',
        cluster_by = ['start_date'],
        incremental_strategy='merge',
    )
}}

with transformed as (
    select
        id                                    as activity_id,
        athlete.id                            as athlete_id,
        name                                  as activity_name,
        sport_type,
        trainer,
        cast(start_date as date)              as start_date,
        cast(distance / 1000 as numeric)      as distance_km,
        cast(moving_time / 60 as numeric)   as moving_time_mins,
        cast(elapsed_time / 60 as numeric)    as elapsed_time_mins,
        cast(total_elevation_gain as numeric) as total_elevation_gain,
        cast(average_speed * 3.6 as numeric)  as average_speed_kmh,
        cast(max_speed * 3.6 as numeric)      as max_speed_kmh,
        cast(average_watts as numeric)        as average_watts,
        cast(average_heartrate as numeric)    as average_heartrate,
        cast(max_heartrate as numeric)        as max_heartrate,
        cast(average_cadence as numeric)      as average_cadence,
        cast(kilojoules as numeric)           as kilojoules,
        cast(suffer_score as numeric)         as suffer_score
    from {{ source('raw_activities', 'ext_activities_raw') }}
    {% if is_incremental() %}
          where 
            cast(start_date as date) > date_sub(current_date(), interval 15 day)
            and has_heartrate = true
    {% endif %}
)

select * from transformed

