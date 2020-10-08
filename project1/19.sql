select count(distinct P.type)
from Gym as G
join CatchedPokemon as CP
on CP.owner_id = G.leader_id
join Pokemon as P
on P.id = CP.pid
where G.city = 'Sangnok City';
