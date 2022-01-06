CREATE TABLE IF NOT EXISTS whale_backcast (
   id UUID PRIMARY KEY,
   model_id UUID,
   datetime_insert TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
   date_backcast_from TIMESTAMP WITH TIME ZONE NOT NULL,
   date_backcast_until TIMESTAMP WITH TIME ZONE NOT NULL,
   metrics JSON
);