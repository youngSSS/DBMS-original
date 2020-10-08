select count(P.type)
from Pokemon as P
join CatchedPokemon as CP
on CP.pid = P.id
group by P.type
order by P.type;