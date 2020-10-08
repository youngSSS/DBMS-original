select T.name, count(T.name)
from Trainer as T
join CatchedPokemon as CP
on T.id = CP.owner_id
where T.hometown = 'Sangnok City'
group by T.name
order by count(T.name);
