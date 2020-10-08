select avg(CP.level)
from Gym as G
join CatchedPokemon as CP
on G.leader_id = CP.owner_id;