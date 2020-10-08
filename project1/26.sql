select P.name
from CatchedPokemon as CP
join Pokemon as P
on P.id = CP.pid
where CP.nickname like '% %'
order by P.name desc;