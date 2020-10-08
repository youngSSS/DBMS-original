select T.name, avg(CP.level)
from Trainer as T
join CatchedPokemon as CP
on CP.owner_id = T.id
join Pokemon as P
on P.id = CP.pid
where P.type in ('Normal', 'Electric')
group by T.name
order by avg(CP.level);