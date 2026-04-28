{{ config(materialized='table') }}

-- Dim Date : générée par CTE récursive couvrant 2018-2019 (données réelles du dataset).

with date_spine as (
    select
        dateadd(day, seq4(), to_date('2018-01-01')) as date_day
    from table(generator(rowcount => 730))   -- 2 ans (2018-2019)
),

enriched as (
    select
        date_day                                        as date_key,
        year(date_day)                                  as year,
        quarter(date_day)                               as quarter,
        month(date_day)                                 as month,
        monthname(date_day)                             as month_name,
        day(date_day)                                   as day,
        dayofweek(date_day)                             as day_of_week,
        dayname(date_day)                               as day_name,
        weekofyear(date_day)                            as week_of_year,
        case when dayofweek(date_day) in (0, 6) then true else false end as is_weekend,
        case
            when month(date_day) in (12, 1, 2)  then 'Winter'
            when month(date_day) in (3, 4, 5)   then 'Spring'
            when month(date_day) in (6, 7, 8)   then 'Summer'
            else 'Fall'
        end                                             as season
    from date_spine
)

select * from enriched
