with source as (
    select * from customers
),
clean as (
    select
        cast(customerID as varchar)  as customer_id,
        trim(companyName)            as company_name,
        trim(contactName)            as contact_name,
        trim(country)                as country
    from source
)
select * from clean

