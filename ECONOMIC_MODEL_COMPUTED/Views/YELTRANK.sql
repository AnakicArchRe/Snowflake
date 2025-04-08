create or replace view YELTRANK(
	RANK,
	PERCENTILE,
	PERCENTILEINV,
	RETURNPERIOD
) as
select 
    seq4()+1 as rank,
    (rank - 1) / 10000 as percentile,
    (10000 - rank) / 10000 as percentileInv,
    10000 / (10000 - rank + 1) as returnPeriod
from 
    table(generator(rowcount => 10000))
;