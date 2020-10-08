select T.name, sum(CP.level)
from CatchedPokemon as CP
join Trainer as T
on T.id = CP.owner_id
group by T.name
order by sum(CP.level) desc limit 1;