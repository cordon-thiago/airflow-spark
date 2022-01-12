select id, model_id, datetime_insert, date_backcast_from, date_backcast_until,
cast(metrics ->> 'rmse' as float) as RMSE,
cast(metrics ->> 'mae' as float) as MAE,
cast(metrics ->> 'mape' as float) as MAPE
from whale_backcast wb
where wb.model_id = {{model_id}}
