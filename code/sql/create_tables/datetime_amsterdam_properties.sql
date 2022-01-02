CREATE TABLE IF NOT EXISTS datetime_amsterdam_properties (
   datetime TIMESTAMP WITH TIME ZONE PRIMARY KEY,
   --datetime_amsterdam TIMESTAMP WITH TIME ZONE NOT NULL,
   week_day SMALLINT NOT NULL, 
   is_holiday BOOLEAN NOT NULL, 
   sfm_sin REAL NOT NULL,
   sfm_cos REAL NOT NULL,
   doy_sin REAL NOT NULL,
   doy_cos REAL NOT NULL
);

SELECT create_hypertable('datetime_amsterdam_properties','datetime', if_not_exists => TRUE);