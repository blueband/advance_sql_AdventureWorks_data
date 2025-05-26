-- Calculate the total order price, currency adjusted total price for non-US countries,
--  and,  total line using salesorderheader and salesorderdetail


select sod.salesorderid "Sales Order ID",
(soh.subtotal + soh.taxamt + soh.freight) "Total Order Price",
soh.currencyrateid "Currency Rate ID",
(soh.subtotal + soh.taxamt + soh.freight) * cr.averagerate "Currency Eqv/Non US",
cr.averagerate "Currency Rate"
,
c.name "Currency Name"
,
(sod.orderqty * sod.unitprice) - (sod.orderqty * sod.unitpricediscount) "Total line Price"
from sales.salesorderheader soh
inner join sales.salesorderdetail sod
on soh.salesorderid = sod.salesorderid
left join sales.currencyrate cr
on soh.currencyrateid = cr.currencyrateid
left join sales.currency c
on cr.tocurrencycode = c.currencycode
