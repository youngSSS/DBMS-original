select A.name, avg(A.level)
from (
  select T.name, CP.level
  from Gym as G
  join CatchedPokemon as CP
  on CP.owner_id = G.leader_id
  join Trainer as T
  on T.id = G.leader_id
) as A
group by A.name
order by A.name;
