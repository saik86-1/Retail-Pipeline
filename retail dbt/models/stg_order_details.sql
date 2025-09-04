with source as (
    select * from order_details
),
clean as (
    select
        cast(order_id   as varchar)  as order_id,
        cast(product_id as varchar)  as product_id,
        try_cast(unit_price as double) as unit_price,
        try_cast(quantity  as integer) as quantity,
        try_cast(discount  as double)  as discount
    from source
)
select * from clean

