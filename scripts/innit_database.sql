/*
=========================================
Create Database and Schemas in PostgreSQL Dialect & Server environment.
=========================================

Purpose: The script creates a new Database "DataWarehouse" with the three Schemas "bronze", "silver", "gold".

WARNING: 
The Script deletes any other Database with the name "DataWarehouse", then recreates the new, empty Database. All data from the deleted Database will be lost!
If needed, create back-ups before running the script.

ERROR HANDLING:
In case of the deletion process not working properly, terminate all connections to the old Database using:

SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname = 'DataWarehouse'
  AND pid <> pg_backend_pid();
DROP DATABASE IF EXISTS "DataWarehouse";

*/

DROP DATABASE IF EXISTS "DataWarehouse";

CREATE DATABASE "DataWarehouse"
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'C'
    LC_CTYPE = 'C'
    LOCALE_PROVIDER = 'libc'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;


-- Create Schemas:
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
