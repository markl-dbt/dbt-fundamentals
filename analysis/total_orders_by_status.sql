with orders_by_status as
(
    select status, count(1) cnt
    from {{ ref('stg_orders')}}
    group by status
)
select * from orders_by_status