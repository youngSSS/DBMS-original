select *
from (
  select P.name, P.id
  from Trainer as T
  join CatchedPokemon as CP
  on CP.owner_id = T.id
  join Pokemon as P
  on P.id = CP.pid
  where T.hometown = 'Sangnok City'
) as A
order by A.id;