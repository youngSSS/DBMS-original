select CP.nickname
from CatchedPokemon as CP
where CP.level > 49 and CP.owner_id > 5
order by CP.nickname;