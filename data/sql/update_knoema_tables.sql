---ALTER TABLE external_data.knoema_burglary DROP COLUMN IF EXISTS country_code;
---ALTER TABLE external_data.knoema_crime DROP COLUMN IF EXISTS country_code;
---ALTER TABLE external_data.knoema_homicide DROP COLUMN IF EXISTS country_code;

ALTER TABLE external_data.knoema_crime ADD COLUMN country_code CHAR(3);
ALTER TABLE external_data.knoema_burglary ADD COLUMN country_code CHAR(3);
ALTER TABLE external_data.knoema_homicide ADD COLUMN country_code CHAR(3);
WITH
update_crime AS (
    UPDATE external_data.knoema_crime
    SET country_code = pc.iso3
    FROM external_data.proper_country AS pc
    WHERE external_data.knoema_crime.country = pc.knoema_country_name
    RETURNING *
),
update_burglary AS (
    UPDATE external_data.knoema_burglary 
    SET country_code = pc.iso3
    FROM external_data.proper_country AS pc
    WHERE external_data.knoema_burglary."location" = pc.knoema_country_name
    RETURNING *
),
update_homicide AS (
    UPDATE external_data.knoema_homicide  
    SET country_code = pc.iso3
    FROM external_data.proper_country AS pc
    WHERE external_data.knoema_homicide.country = pc.knoema_country_name
    RETURNING *
)
SELECT 'knoema_crime' AS table_name, COUNT(*) AS updated_rows FROM update_crime
UNION ALL
SELECT 'knoema_burglary' AS table_name, COUNT(*) AS updated_rows FROM update_burglary
UNION ALL
SELECT 'knoema_homicide' AS table_name, COUNT(*) AS updated_rows FROM update_homicide;