with o as (
    -- orders with day bucket
    select
        cast(date_trunc('day', order_ts) as date) as order_date,
        order_id,
        customer_id
    from {{ ref('stg_orders') }}
    where order_ts is not null
),
firsts as (
    -- first order day for each customer
    select
        customer_id,
        min(cast(date_trunc('day', order_ts) as date)) as first_order_date
    from {{ ref('stg_orders') }}
    where order_ts is not null
    group by 1
),
joined as (
    -- label new vs repeat per day
    select
        o.order_date,
        o.customer_id,
        f.first_order_date
    from o
    join firsts f using (customer_id)
),
daily_base as (
    -- daily counts
    select
        order_date,
        count(*)                                                    as order_count,
        count(distinct customer_id)                                 as customer_count,
        count(distinct case when order_date = first_order_date
                            then customer_id end)                   as new_customers,
        count(distinct case when order_date > first_order_date
                            then customer_id end)                   as repeat_customers
    from joined
    group by 1
),
revenue_by_day as (
    -- sum revenue by order day
    select
        cast(date_trunc('day', s.order_ts) as date) as order_date,
        sum(r.order_revenue)                        as total_revenue
    from {{ ref('stg_orders') }} s
    join {{ ref('int_order_revenue') }} r using (order_id)
    where s.order_ts is not null
    group by 1
)
select
    b.order_date,
    b.order_count,
    b.customer_count,
    b.new_customers,
    b.repeat_customers,
    coalesce(r.total_revenue, 0.0)                           as total_revenue,
    case when b.order_count > 0
         then round(coalesce(r.total_revenue, 0.0) / b.order_count, 2)
         else null
    end                                                      as aov
from daily_base b
left join revenue_by_day r using (order_date)
order by 1

