select 
	dap.datetime ,
	case
	when dap.week_day in (5, 6)
	or dap.is_holiday = true then false
	else true end as is_working_day,
	dap.sfm_sin , 
	dap.sfm_cos ,
	dap.doy_sin ,
	dap.doy_cos ,
	kwh.temperature ,
	kwh.solar_radiation ,
	whp.consumption
from
	datetime_amsterdam_properties dap
left join
	whale_historical_ptu whp
	on
	dap.datetime = whp.datetime
left join knmi_weather_hour kwh 
	on
	dap.datetime = kwh.datetime
where
	dap.datetime between {{date_from}} and {{date_until}}
	and whp.connection_id = {{connection_id}}


-- trim away the nulls at start/end, but not eventual nulls in between
/*
left join (
select
	connection_id,
	min(datetime) as dttm_from,
	max(datetime) as dttm_until
from
	(
	select
		*
	from
		whale_historical_ptu
	where
		consumption is not null
		and datetime between '2019-01-01' and '2022-01-01'
		and connection_id = '871685900000000059MV'
	)as tbl_not_null
group by
	connection_id
) tbl_ranges
	on
whp.connection_id = tbl_ranges.connection_id
where
whp.datetime >= tbl_ranges.dttm_from
and whp.datetime <= tbl_ranges.dttm_until
*/