select A.owner_id, A.cnt
from (
  select CP.owner_id, count(CP.owner_id) as cnt
  from CatchedPokemon as CP
  group by CP.owner_id
  order by count(CP.owner_id) desc
) as A, (
  select count(CP.owner_id) as max_cnt
  from CatchedPokemon as CP
  group by CP.owner_id
  order by count(CP.owner_id) desc limit 1
) as B
where A.cnt = B.max_cnt
order by A.owner_id;