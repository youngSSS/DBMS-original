select P.name, CP.level, CP.nickname
from Gym as G
join CatchedPokemon as CP
on CP.owner_id = G.leader_id
join Pokemon as P
on P.id = CP.pid
where CP.nickname like 'A%'
order by P.name desc;