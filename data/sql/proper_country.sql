DROP VIEW IF EXISTS external_data.proper_country;
CREATE OR REPLACE VIEW external_data.proper_country
AS SELECT c.id, c.iso, c.name, c.iso3, COALESCE(m.country_name_in_knoema_table, c.name) AS knoema_country_name
FROM external_data.country_data AS c
FULL JOIN external_data.country_name_mapping AS m
ON c."name" = m.country_name_in_country_table;