--  Find customer who have not made purchase within a period 2011-01-01 and 2011-12-31
-- tables : customer, saleorderhead, person


with all_2011_order as (
select 
*
from person.person p
inner join sales.customer c 
on p.businessentityid  = c.personid
left join sales.salesorderheader s 
on c.customerid = s.customerid and orderdate between '2011-01-01' and '2011-12-31'
)

select * 
from all_2011_order
where salesorderid is null 