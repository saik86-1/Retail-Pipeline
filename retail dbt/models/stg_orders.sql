with source as (
    select * from orders
),

clean as (
    select
        -- ids
        cast(orderID    as varchar)   as order_id,
        cast(customerID as varchar)   as customer_id,
        cast(employeeID as varchar)   as employee_id,

        -- dates (use try_cast so bad values become NULL instead of errors)
        try_cast(orderDate    as timestamp) as order_ts,
        try_cast(requiredDate as timestamp) as required_ts,
        try_cast(shippedDate  as timestamp) as shipped_ts,

        -- shipping + cost
        cast(shipVia as varchar)      as ship_via,
        try_cast(freight as double)   as freight,

        -- shipping address (trim to tidy)
        trim(shipName)        as ship_name,
        trim(shipAddress)     as ship_address,
        trim(shipCity)        as ship_city,
        trim(shipRegion)      as ship_region,
        trim(shipPostalCode)  as ship_postal_code,
        trim(shipCountry)     as ship_country
    from source
)

select * from clean

