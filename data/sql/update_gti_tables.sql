---ALTER TABLE external_data.global_terrorism_index_2022 DROP COLUMN IF EXISTS country_code;
---ALTER TABLE external_data.global_terrorism_index_2023 DROP COLUMN IF EXISTS country_code;

ALTER TABLE external_data.global_terrorism_index_2022 ADD COLUMN country_code CHAR(3);
ALTER TABLE external_data.global_terrorism_index_2023 ADD COLUMN country_code CHAR(3);
ALTER TABLE external_data.global_terrorism_db ADD COLUMN country_code CHAR(3);
WITH
GTI_2022 AS (
    UPDATE external_data.global_terrorism_index_2022
    SET country_code = pc.iso3
    FROM external_data.proper_country AS pc
    WHERE external_data.global_terrorism_index_2022.country = pc.knoema_country_name
    RETURNING *
),
GTI_2023 AS (
    UPDATE external_data.global_terrorism_index_2023
    SET country_code = pc.iso3
    FROM external_data.proper_country AS pc
    WHERE external_data.global_terrorism_index_2023.country = pc.knoema_country_name
    RETURNING *
),
GTIDB AS (
    UPDATE external_data.global_terrorism_db
    SET country_code = pc.iso3
    FROM external_data.proper_country AS pc
    WHERE external_data.global_terrorism_db.country_txt = pc.knoema_country_name
    RETURNING *
)
SELECT 'global_terrorism_index_2022' AS table_name, COUNT(*) AS updated_rows FROM GTI_2022
UNION ALL
SELECT 'global_terrorism_index_2023' AS table_name, COUNT(*) AS updated_rows FROM GTI_2023
UNION ALL
SELECT 'global_terrorism_index_2023' AS table_name, COUNT(*) AS updated_rows FROM GTIDB