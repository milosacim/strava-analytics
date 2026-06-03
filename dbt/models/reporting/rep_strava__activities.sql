with cte as( 
    select 
        DATE_TRUNC(ac.start_date, ISOWEEK) as date,
        extract(year from ac.start_date) as year,
        extract(month from ac.start_date) as month,
        CONCAT('W', CAST(extract(ISOWEEK from ac.start_date) as string)) as week,
        ac.athlete_id,
        ac.sport_type,
        ac.distance_km,
        ac.moving_time_mins,
        ac.elapsed_time_mins,
        ac.total_elevation_gain,
        ac.average_speed_kmh,
        ac.average_heartrate,
        ac.average_cadence,
        ac.kilojoules,
        ac.suffer_score
    from {{ ref('stg_strava__activities') }}
)
select 
    date,
    year,
    month,
    week,
    athlete_id,
    sport_type,
    sum(distance_km) as distance_km,
    sum(moving_time_mins) as moving_time_mins,
    sum(elapsed_time_mins) as elapsed_time_mins,
    sum(total_elevation_gain) as total_elevation_gain,
    avg(average_speed_kmh) as average_speed_kmh,
    avg(average_heartrate) as average_heartrate,
    avg(average_cadence) as average_cadence,
    avg(kilojoules) as average_kilojoules,
    avg(suffer_score) as average_suffer_score
from cte
group by 
    date, 
    year,
    month,
    week,
    sport_type, 
    athlete_id