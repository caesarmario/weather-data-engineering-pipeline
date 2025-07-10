with source as (
  select * from raw.current
),

renamed as (
  select
    location_id,
    date,
    last_updated,
    temp_c,
    temp_f,
    is_day,
    condition_text,
    condition_code,
    batch_id,
    load_dt
  from source
)

select * from renamed