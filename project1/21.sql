select T.name, count(T.name)
from Gym as G
join CatchedPokemon as CP
on CP.owner_id = G.leader_id
join Trainer as T
on T.id = G.leader_id
group by T.name
order by T.name;