select A.name
from (
  select P.name
  from Pokemon as P
  join Evolution as E
  on E.before_id = P.id
  where P.type = 'Grass'
) as A;