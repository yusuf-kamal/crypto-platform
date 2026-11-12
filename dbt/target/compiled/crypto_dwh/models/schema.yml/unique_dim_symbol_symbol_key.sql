
    
    

select
    symbol_key as unique_field,
    count(*) as n_records

from "CryptoDWH"."gold"."dim_symbol"
where symbol_key is not null
group by symbol_key
having count(*) > 1


