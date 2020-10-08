select avg(A.level)
from (
  select CP.level, type
  from Pokemon as P
  join CatchedPokemon as CP
  on CP.pid = P.id
  where CP.owner_id in (
     select id
     from Trainer as T
     where T.hometown = 'Sangnok City'
     )
  ) as A
where A.type = 'Electric';