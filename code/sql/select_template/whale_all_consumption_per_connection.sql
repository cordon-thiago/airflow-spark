select datetime, consumption
from whale_historical_ptu whp
where connection_id = {{connection_id}}
and consumption is not null