select A.name
from (
  select T.name, count(T.name) as cnt_name
  from Trainer as T
  join CatchedPokemon as CP
  on CP.owner_id = T.id
  group by T.name
  order by cnt_name desc
) as A
where A.cnt_name > 2;