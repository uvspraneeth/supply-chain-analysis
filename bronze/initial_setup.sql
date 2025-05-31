-- Drop the database if already existed 'supplychain' (note: must do outside the DB)
DROP DATABASE IF EXISTS supplychain;
-- Create database 'supplycahin'
CREATE DATABASE supplychain;

-- connect to DB (i.e, supplycahin) manual or using psql/pgadmin
-- \c supplychain (psql)

-- Create schema (run after connected to the database 'supplychain')
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;

