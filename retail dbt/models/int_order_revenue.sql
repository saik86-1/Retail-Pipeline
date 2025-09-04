with lines as (
    select
        order_id,
        try_cast(quantity   as double)  as qty,
        try_cast(unit_price as double)  as price,
        coalesce(try_cast(discount as double), 0.0) as discount
    from {{ ref('stg_order_details') }}
),
calc as (
    select
        order_id,
        sum(qty * price * (1 - discount)) as order_revenue,
        sum(qty)                           as total_qty,
        count(*)                           as line_items
    from lines
    group by 1
)
select * from calc

