-- not really used right now
CREATE TABLE IF NOT EXISTS ean_code (
   ean_code VARCHAR(20) NOT NULL, 
   customer_id VARCHAR(10) NOT NULL,
   label VARCHAR(20) NOT NULL,
   ean_code_grid_operator INTEGER
   
   -- UNIQUE (ean_code ...)
);