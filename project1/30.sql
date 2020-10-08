select E1.before_id as e_1, P1.name as p1, 
P2.name as p2, P3.name as p3
from Evolution as E1
join Evolution as E2
on E2.before_id = E1.after_id
join Pokemon as P1
on E1.before_id = P1.id
join Pokemon as P2
on E2.before_id = P2.id
join Pokemon as P3
on E2.after_id = P3.id
order by e_1;