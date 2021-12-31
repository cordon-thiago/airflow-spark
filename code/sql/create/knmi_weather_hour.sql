CREATE TABLE IF NOT EXISTS knmi_weather_hour (
   station_id INT NOT NULL,
   datetime TIMESTAMP WITH TIME ZONE NOT NULL,
   temperature DOUBLE PRECISION,
   solar_radiation DOUBLE PRECISION,
   UNIQUE (station_id, datetime)
);

SELECT create_hypertable('knmi_weather_hour','datetime', if_not_exists => TRUE);