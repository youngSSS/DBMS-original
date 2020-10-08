select C.name, avg(CP.level)
from City as C
join Trainer as T
on T.hometown = C.name
join CatchedPokemon as CP
on CP.owner_id = T.id
group by C.name
order by avg(CP.level);