select count(*)
from (
  select distinct CP.pid
  from Trainer as T
  join CatchedPokemon as CP
  on CP.owner_id = T.id
  where T.hometown = 'Sangnok City'
) as A;