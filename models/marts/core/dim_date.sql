{{
  config(
    tags=['all']
    )
}}

with date_spine as 
(

  {{ dbt_utils.date_spine(
    datepart="day"
    , start_date="cast('2019-01-01' as date)"
    , end_date="cast('2020-01-01' as date)"
     )
  }}

)
, calculated as 
(
  select date(date_day) as dim_date_key
  , date(date_day) as date_actual
  , extract(month from date_day)  as month_actual
  , extract(year from date_day) as year_actual
  , extract(quarter from date_day) as quarter_actual
  , extract(dayofweek from date_day) as day_of_week
  , extract(week from date_day) as week_of_year
  , extract(day from date_day) as day_of_month
  , row_number() over (partition by extract(year from date_day), extract(quarter from date_day) order by date_day) as day_of_quarter
  , extract(dayofyear from date_day) as day_of_year
  , case when extract(month from date_day) < 2   then extract(year from date_day) else (extract(year from date_day)+1) end as fiscal_year
  , case when extract(month from date_day) < 2 then '4'
      when extract(month from date_day) < 5 then '1'
      when extract(month from date_day) < 8 then '2'
      when extract(month from date_day) < 11 then '3'
      else '4' 
    end as fiscal_quarter
  --, row_number() over (partition by fiscal_year, fiscal_quarter order by date_day) as day_of_fiscal_quarter
  --, row_number() over (partition by fiscal_year order by date_day) as day_of_fiscal_year
  , date_trunc(date(date_day), week) as first_day_of_week
  , last_day(date_day, week) as last_day_of_week
  , date_trunc(date(date_day), month) as first_day_of_month
  , last_day(date_day, month) as last_day_of_month
  , date_trunc(date(date_day), year) as first_day_of_year
  , last_day(date_day, year) as last_day_of_year
  , first_value(date(date_day)) over (partition by extract(year from date_day), extract(quarter from date_day) order by date_day) as first_day_of_quarter
  , last_day(date_day, quarter) as last_day_of_quarter
  --, first_value(date_day) over (partition by fiscal_year, fiscal_quarter order by date_day) as first_day_of_fiscal_quarter
  --, last_value(date_day) over (partition by fiscal_year, fiscal_quarter order by date_day) as last_day_of_fiscal_quarter
  --, first_value(date_day) over (partition by fiscal_year order by date_day) as first_day_of_fiscal_year
  --, last_value(date_day) over (partition by fiscal_year order by date_day) as last_day_of_fiscal_year
  --, datediff('week', first_day_of_fiscal_year, date_actual) +1 as week_of_fiscal_year
  , case when extract( month from date_day) = 1 then 12 else extract( MONTH from date_day) - 1 end as month_of_fiscal_year
  , format_date('%A', date_day) as day_name
  , format_date('%B', date_day) as month_name
  , (extract(year from date_day) || '-q' || extract(quarter from date_day)) as quarter_name
  --, (fiscal_year || '-' || decode(fiscal_quarter, 1, 'q1', 2, 'q2', 3, 'q3', 4, 'q4')) as fiscal_quarter_name
  --, ('fy' || substr(fiscal_quarter_name, 3, 7)) as fiscal_quarter_name_fy
  --, dense_rank() over (order by fiscal_quarter_name) as fiscal_quarter_number_absolute
  --, fiscal_year || '-' || monthname(date_day) as fiscal_month_name
  --, ('fy' || substr(fiscal_month_name, 3, 8)) as fiscal_month_name_fy
  , (
      case when extract(month from date_day) = 1 and extract(day from date_day) = 1 then 'new years day'
        when extract(month from date_day) = 12 and extract(day from date_day) = 25 then 'christmas day'
        when extract(month from date_day) = 12 and extract(day from date_day) = 26 then 'boxing day'
        else null 
      end
    ) as holiday_desc
  , case when
    (
      case when extract(month from date_day) = 1 and extract(day from date_day) = 1 then 'new years day'
        when extract(month from date_day) = 12 and extract(day from date_day) = 25 then 'christmas day'
        when extract(month from date_day) = 12 and extract(day from date_day) = 26 then 'boxing day'
        else null 
      end 
    ) is null then false else true end as is_holiday
  --, date_trunc('month', last_day_of_fiscal_quarter) as last_month_of_fiscal_quarter
  --, iff(date_trunc('month', last_day_of_fiscal_quarter) = date_actual, true, false) as is_first_day_of_last_month_of_fiscal_quarter
  --, date_trunc('month', last_day_of_fiscal_year) as last_month_of_fiscal_year
  --, iff(date_trunc('month', last_day_of_fiscal_year) = date_actual, true, false) as is_first_day_of_last_month_of_fiscal_year
  --, date_add('day',7,date_add('month',1,first_day_of_month)) as snapshot_date_fp
  from date_spine
)
, final as 
(
  select dim_date_key
  , date_actual
  , month_actual
  , year_actual
  , quarter_actual
  , day_of_week
  , week_of_year
  , day_of_month
  , day_of_quarter
  , day_of_year
  , fiscal_year
  , fiscal_quarter
  --, day_of_fiscal_quarter
  --, day_of_fiscal_year
  , day_name
  , month_name
  , quarter_name
  , first_day_of_week
  , last_day_of_week
  , first_day_of_month
  , last_day_of_month
  , first_day_of_year
  , last_day_of_year
  , first_day_of_quarter
  , last_day_of_quarter
  --, first_day_of_fiscal_quarter
  --, last_day_of_fiscal_quarter
  --, first_day_of_fiscal_year
  --, last_day_of_fiscal_year
  --, week_of_fiscal_year
  --, month_of_fiscal_year
  --, fiscal_quarter_name
  --, fiscal_quarter_name_fy
  --, fiscal_quarter_number_absolute
  --, fiscal_month_name
  --, fiscal_month_name_fy
  , holiday_desc
  , is_holiday
  --, last_month_of_fiscal_quarter
  --, is_first_day_of_last_month_of_fiscal_quarter
  --, last_month_of_fiscal_year
  --, is_first_day_of_last_month_of_fiscal_year
  --, snapshot_date_fpa
  from calculated
)

select * from final