select *
from (
  select T.name, max(CP.level)
  from Trainer as T
  join CatchedPokemon as CP
  on CP.owner_id = T.id
  group by T.name
  having count(T.name) > 3
) as A
order by A.name;