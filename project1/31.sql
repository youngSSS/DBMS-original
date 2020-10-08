select P.type
from Evolution as E
join Pokemon as P
on P.id = E.before_id
group by P.type
having count(P.type) > 2
order by P.type desc;