CREATE TABLE IF NOT EXISTS whale_historical_ptu (
   connection_id VARCHAR(20) NOT NULL,
   datetime TIMESTAMP WITH TIME ZONE NOT NULL,
   consumption DOUBLE PRECISION,
   UNIQUE (connection_id, datetime)
);

SELECT create_hypertable('whale_historical_ptu','datetime', if_not_exists => TRUE);

-- create index?
create index on whale_historical_ptu (datetime);