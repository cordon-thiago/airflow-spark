-- not really used right now
CREATE TABLE IF NOT EXISTS bird_historical_meter (
   datetime TIMESTAMP WITH TIME ZONE NOT NULL,
   value DOUBLE PRECISION,
   channel VARCHAR(3) NOT NULL,
   product VARCHAR(15) NOT NULL,
   ean_code VARCHAR(20) NOT NULL,
   
   UNIQUE (ean_code, channel, datetime)
);

SELECT create_hypertable('bird_historical_meter','datetime', if_not_exists => TRUE);

-- create index?
create index on bird_historical_meter (datetime);