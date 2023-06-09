insert into raw.jaffle_shop.employees (id, first_name, last_name)
select top 100 id, first_name, last_name from raw.jaffle_shop.customers
