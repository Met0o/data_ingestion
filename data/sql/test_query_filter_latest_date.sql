CREATE OR REPLACE VIEW external_data.worldbank_middle_east_2
AS
WITH latest_years AS (
    SELECT metric, MAX("year") AS latest_year
    FROM external_data.worldbank_crime_terror
    WHERE (country = ANY (ARRAY['Saudi Arabia'::text, 'United Arab Emirates'::text, 'Bahrain'::text, 'Kuwait'::text, 'Qatar'::text, 'Oman'::text]))
    AND (metric = ANY (ARRAY['Intentional homicides (per 100,000 people)'::text,'Crime (% of managers surveyed ranking this as a major constraint)'::text,'Intentional homicides, female (per 100,000 female)'::text,'Percent of firms choosing crime, theft and disorder as their biggest obstacle'::text,'Safety and Rule of Law'::text,'Intentional homicides, male (per 100,000 male)'::text,'Political Stability/No Violence (estimate)'::text,'Percent of firms identifying crime, theft and disorder as a major constraint'::text]))
    GROUP BY metric
)
SELECT
    wb.country,
    wb.country_code,
    wb."year",
    wb.metric,
    CASE
        WHEN wb.value = 'NaN'::numeric THEN 0
        ELSE wb.value
    END AS value
FROM external_data.worldbank_crime_terror AS wb
JOIN latest_years AS ly ON wb.metric = ly.metric AND wb."year" = ly.latest_year
WHERE (wb.country = ANY (ARRAY['Saudi Arabia'::text, 'United Arab Emirates'::text, 'Bahrain'::text, 'Kuwait'::text, 'Qatar'::text, 'Oman'::text]))
AND (wb.metric = ANY (ARRAY['Intentional homicides (per 100,000 people)'::text,'Crime (% of managers surveyed ranking this as a major constraint)'::text,'Intentional homicides, female (per 100,000 female)'::text,'Percent of firms choosing crime, theft and disorder as their biggest obstacle'::text,'Safety and Rule of Law'::text,'Intentional homicides, male (per 100,000 male)'::text,'Political Stability/No Violence (estimate)'::text,'Percent of firms identifying crime, theft and disorder as a major constraint'::text]))
