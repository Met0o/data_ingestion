---ALTER TABLE external_data.worldbank_crime_terror DROP COLUMN IF EXISTS country_code;

ALTER TABLE external_data.worldbank_crime_terror ADD COLUMN country_code CHAR(3);
WITH
update_worldbank AS (
    UPDATE external_data.worldbank_crime_terror
    SET country_code = pc.iso3
    FROM external_data.proper_country AS pc
    WHERE external_data.worldbank_crime_terror.country = pc."name" 
    RETURNING *
)
SELECT 'worldbank_crime_terror' AS table_name, COUNT(*) AS updated_rows FROM update_worldbank;