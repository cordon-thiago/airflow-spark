select
	connection_id,
	min(datetime) as dttm_from,
	max(datetime) as dttm_until,
	min(datetime) + ((max(datetime) - min(datetime)) * 0.8) as dttm_08
from
	(
	select
		*
	from
		whale_historical_ptu
	where
		consumption is not null 
) as cons
group by
	connection_id