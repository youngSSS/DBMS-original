select P2.name
from (
  select count(P.type) as cnt_a, P.type
  from Pokemon as P
  group by P.type
  ) as A
join (
  select count(P1.type) as cnt_b
  from Pokemon as P1
  group by P1.type
  order by count(P1.type) desc limit 2
  )as B
on A.cnt_a = B.cnt_b
join Pokemon as P2
on P2.type = A.type
order by P2.name;