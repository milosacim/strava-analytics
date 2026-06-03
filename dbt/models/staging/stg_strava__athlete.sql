
-- Silver: one row per Strava athlete. Rebuilt fully each run (source is a 1-row API response).
{{
    config(
        alias='stg_athlete',
        unique_key='athlete_id',
    )
}}

select
    id as athlete_id,
    firstname as first_name,
    lastname as last_name,
    city,
    state,
    country
from {{ source('raw_athlete', 'ext_athlete_raw') }}
    