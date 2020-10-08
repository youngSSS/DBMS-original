select distinct T.name
from Trainer as T
join CatchedPokemon as CP
on CP.owner_id = T.id
where CP.level < 11
order by T.name;