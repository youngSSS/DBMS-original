select distinct A.name, A.type
from (
  select P.name, P.type
  from CatchedPokemon as CP
  join Pokemon as P
  on P.id = CP.pid
  where CP.level > 29
) as A
order by A.name;