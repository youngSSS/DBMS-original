select P.name
from (
  select E.before_id
  from Evolution as E
  where E.before_id > E.after_id
) as A
join Pokemon as P
on P.id = A.before_id
order by P.name;