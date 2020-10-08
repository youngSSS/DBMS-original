select P.type, count(P.type)
from Pokemon as P
group by P.type
order by count(P.type), P.type;