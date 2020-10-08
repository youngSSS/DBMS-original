select P.name
from (
  select E.after_id
  from Evolution as E
  where E.after_id not in (
    select E1.before_id
    from Evolution as E1
  )
) as A
join Pokemon as P
on P.id = A.after_id
order by P.name;