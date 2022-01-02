CREATE TABLE IF NOT EXISTS bird_historical_meter_raw (
   ean_code_grid_operator BIGINT,
   event_timestamp TIMESTAMP WITH TIME ZONE,
   measurement_timestamp TIMESTAMP WITH TIME ZONE,
   measurement_value DOUBLE PRECISION,
   deleted BOOLEAN,
   event_instance_id VARCHAR(50),
   channel VARCHAR(3),
   label VARCHAR(20),
   customer_id VARCHAR(20),
   ean_code VARCHAR(20),
   collector VARCHAR (3),
   product VARCHAR(15)
);

SELECT create_hypertable('bird_historical_meter_raw','measurement_timestamp', if_not_exists => TRUE);

-- create index?
create index on bird_historical_meter_raw (measurement_timestamp);