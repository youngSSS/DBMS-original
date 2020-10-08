select count(*)
from Pokemon as P
where P.type = 'Water' or P.type = 'Electric' or P.type = 'Psychic';