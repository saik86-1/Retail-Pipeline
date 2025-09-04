with orders as (
    select
        order_id,
        customer_id,
        order_ts
    from {{ ref('stg_orders') }}
    where order_ts is not null
),
revenue as (
    select
        order_id,
        order_revenue
    from {{ ref('int_order_revenue') }}
),
joined as (
    select
        o.customer_id,
        o.order_ts,
        coalesce(r.order_revenue, 0.0) as order_revenue
    from orders o
    left join revenue r using (order_id)
),
agg as (
    select
        customer_id,
        count(*)                      as order_count,
        min(order_ts)                 as first_order_ts,
        max(order_ts)                 as last_order_ts,
        sum(order_revenue)            as total_revenue
    from joined
    group by 1
)

select
    c.customer_id,
    c.company_name,
    c.contact_name,
    c.country,
    coalesce(a.order_count, 0)        as order_count,
    a.first_order_ts,
    a.last_order_ts,
    coalesce(a.total_revenue, 0.0)    as total_revenue,
    case
      when coalesce(a.order_count,0) > 0
      then round(coalesce(a.total_revenue,0.0) / a.order_count, 2)
      else null
    end                                 as aov,
    case when coalesce(a.order_count,0) > 1 then true else false end as is_repeat_customer
from {{ ref('stg_customers') }} c
left join agg a
  on a.customer_id = c.customer_id

