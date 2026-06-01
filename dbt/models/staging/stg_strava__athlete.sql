with source as (
        select * from {{ source('raw_athlete', 'ext_athlete_raw') }}
  ),
  renamed as (
      select
          id as athlete_id,
          firstname as first_name,
          lastname as last_name,
          city,
          state,
          country
      from source
  )
  select * from renamed
    