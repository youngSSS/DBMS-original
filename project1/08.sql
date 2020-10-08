select avg(A.level)
from (
  select CP.level
  from Trainer as T
  join CatchedPokemon as CP
  on CP.owner_id = T.id
  where T.name = 'Red'
) as A;