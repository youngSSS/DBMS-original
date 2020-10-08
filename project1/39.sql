select T.name
from CatchedPokemon as CP1
join CatchedPokemon as CP2
on CP1.owner_id = CP2.owner_id and CP1.pid = CP2.pid and CP1.id != CP2.id
join Trainer as T
on T.id = CP1.owner_id
group by T.name
order by T.name;