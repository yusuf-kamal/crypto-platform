
    
    

with child as (
    select symbol_key as from_field
    from "CryptoDWH"."gold"."fact_candles_1m"
    where symbol_key is not null
),

parent as (
    select symbol_key as to_field
    from "CryptoDWH"."gold"."dim_symbol"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


