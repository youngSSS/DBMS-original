select P.name
from Pokemon as P
where P.id not in (
  select CP.pid
  from CatchedPokemon as CP
)
order by P.name;