select T.hometown, CP.nickname
from Trainer as T
join CatchedPokemon as CP
on CP.owner_id = T.id
where CP.level in (
  select max(CP.level)
  from Trainer as T
  join CatchedPokemon as CP
  on CP.owner_id = T.id
  group by T.hometown
)
group by T.hometown
order by T.hometown;