-- external_data.unodc_middle_east_data source

CREATE OR REPLACE VIEW external_data.unodc_middle_east
AS SELECT 'homicide'::text AS table_name,
    unodc_homicide.iso3_code,
    unodc_homicide.country,
    unodc_homicide.indicator,
    unodc_homicide.dimension,
    unodc_homicide.category,
    unodc_homicide.sex,
    unodc_homicide.year::date AS year,
    unodc_homicide.measurement,
    round(unodc_homicide.value::numeric, 0) AS value
   FROM external_data.unodc_homicide
  WHERE unodc_homicide.country = ANY (ARRAY['Saudi Arabia'::text, 'United Arab Emirates'::text, 'Bahrain'::text, 'Kuwait'::text, 'Qatar'::text, 'Oman'::text])
UNION ALL
 SELECT 'violent_crime'::text AS table_name,
    unodc_violent_crime.iso3_code,
    unodc_violent_crime.country,
    unodc_violent_crime.indicator,
    unodc_violent_crime.dimension,
    unodc_violent_crime.category,
    unodc_violent_crime.sex,
    unodc_violent_crime.year::date AS year,
    unodc_violent_crime.measurement,
    round(unodc_violent_crime.value::numeric, 0) AS value
   FROM external_data.unodc_violent_crime
  WHERE unodc_violent_crime.country = ANY (ARRAY['Saudi Arabia'::text, 'United Arab Emirates'::text, 'Bahrain'::text, 'Kuwait'::text, 'Qatar'::text, 'Oman'::text])
UNION ALL
 SELECT 'corruption'::text AS table_name,
    unodc_corruption.iso3_code,
    unodc_corruption.country,
    unodc_corruption.indicator,
    unodc_corruption.dimension,
    unodc_corruption.category,
    unodc_corruption.sex,
    unodc_corruption.year::date AS year,
    unodc_corruption.measurement,
    round(unodc_corruption.value::numeric, 0) AS value
   FROM external_data.unodc_corruption
  WHERE unodc_corruption.country = ANY (ARRAY['Saudi Arabia'::text, 'United Arab Emirates'::text, 'Bahrain'::text, 'Kuwait'::text, 'Qatar'::text, 'Oman'::text]);