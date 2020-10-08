select distinct A.name
from (
  select P.id as A_pid, P.name
  from Trainer as T
  join CatchedPokemon as CP
  on CP.owner_id = T.id
  join Pokemon as P
  on P.id = CP.pid
  where T.hometown = 'Sangnok City'
) as A, 
(
  select P.id as B_pid, P.name
  from Trainer as T
  join CatchedPokemon as CP
  on CP.owner_id = T.id
  join Pokemon as P
  on P.id = CP.pid
  where T.hometown = 'Brown City'
) as B
where A.A_pid = B.B_pid
order by A.name;