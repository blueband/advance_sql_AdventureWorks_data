-- retrieve employees that has it last name containg 'MI' or 'TH' and home city is Kenmore

select 
p.persontype as "Person Type",
p.title Title,
p.firstname "First Name",
p.middlename "Middle Name",
p.lastname "Last Name",
p.modifieddate "Modified Date",
a.city,
a2.name

from person.person p inner join 
person.businessentityaddress b 
on p.businessentityid = b.businessentityid
inner join person.addresstype a2 
on b.addresstypeid = a2.addresstypeid 
--and
--a2.name = 'Home'
inner join person.address a 
on b.addressid = a.addressid 
--and
--a.city = 'Kenmore'

where 
(p.lastname like '%mi%' or 
p.lastname like '%th%') and
p.persontype = 'EM' and a2.name = 'Home' and a.city = 'Kenmore'