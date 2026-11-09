
with source_records as (

    select
        trade_observation_id,
        period_start_date,
        reporter_code,
        cmd_code,
        import_value

    from {{ ref('fct_trade_imports') }}

    where query_type = 'world_total'
      and partner_code = 0

),

mart_records as (

    select
        trade_observation_id,
        period_start_date,
        reporter_code,
        cmd_code,
        import_value,
        mom_status,
        yoy_status,
        mom_growth_rate,
        yoy_growth_rate,
        previous_month_value,
        previous_year_value

    from {{ ref('mart_import_trends') }}

),

compared as (

    select
        s.trade_observation_id as source_id,
        m.trade_observation_id as mart_id,

        s.import_value as source_value,
        m.import_value as mart_value,

        s.period_start_date as source_period,
        m.period_start_date as mart_period,

        s.reporter_code as source_reporter,
        m.reporter_code as mart_reporter,

        s.cmd_code as source_cmd,
        m.cmd_code as mart_cmd,

        m.mom_status,
        m.yoy_status,
        m.mom_growth_rate,
        m.yoy_growth_rate,
        m.previous_month_value,
        m.previous_year_value

    from source_records as s

    full outer join mart_records as m
      on s.trade_observation_id = m.trade_observation_id

)

select *
from compared

where source_id is null
   or mart_id is null

   or source_value is distinct from mart_value
   or source_period is distinct from mart_period
   or source_reporter is distinct from mart_reporter
   or source_cmd is distinct from mart_cmd

   or (
       mom_status != 'COMPARABLE'
       and mom_growth_rate is not null
   )

   or (
       yoy_status != 'COMPARABLE'
       and yoy_growth_rate is not null
   )

   or (
       mom_status = 'COMPARABLE'
       and (
           previous_month_value <= 0
           or mom_growth_rate is null
       )
   )

   or (
       yoy_status = 'COMPARABLE'
       and (
           previous_year_value <= 0
           or yoy_growth_rate is null
       )
   )
