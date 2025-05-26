
-- HAVING Concept
-- it can ONLY be use along with aggregation function
-- it is use at very end of query , not by FROM or WHERE clause
--Example 14, Summarize the dataset from above example, so we can see one record per territory. 
-- We can only see territoies with over 50 sales transaction and $1M in sales, dispaly same filed as Example 13

with all_2011_order as (
select 
c.customerid,
c.accountnumber,
p.firstname "First Name",
p.lastname "Last Name",
s.orderdate ,
s.totaldue,
c.territoryid
from person.person p
inner join sales.customer c 
on p.businessentityid  = c.personid
left join sales.salesorderheader s 
on c.customerid = s.customerid and orderdate between '2011-01-01' and '2011-12-31'
where salesorderid is not null 
)
,
region as (
select * from sales.salesterritory
)

select r.name, 
count(al.customerid) total_customer,
count(distinct al.customerid) unique_customer,
round(sum(al.totaldue), 2) "Total spent in Region",
count(al.totaldue) "Total sales in Region",
round(avg(al.totaldue), 2) "Average spent in Region",
round(max(al.totaldue),2) "Maximun spent in Region",
round(min(al.totaldue), 2) "Minimun spent in Region"
from all_2011_order al
inner join region r
on al.territoryid = r.territoryid
group by r.name
having count(distinct al.customerid) >= 50 and round(sum(al.totaldue), 2) >= 1000000
