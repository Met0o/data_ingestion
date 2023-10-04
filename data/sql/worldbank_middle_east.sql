-- external_data.worldbank_middle_east source

CREATE OR REPLACE VIEW external_data.worldbank_middle_east
AS SELECT sq.country_code,
    sq.country,
    sq.metric,
    sq.year,
    sq.value
   FROM ( SELECT worldbank_crime_terror.country_code,
            worldbank_crime_terror.country,
            worldbank_crime_terror.metric,
            worldbank_crime_terror.year,
                CASE
                    WHEN worldbank_crime_terror.value = 'NaN'::numeric THEN NULL::numeric
                    ELSE round(worldbank_crime_terror.value, 2)
                END AS value
           FROM external_data.worldbank_crime_terror
          WHERE (worldbank_crime_terror.country = ANY (ARRAY['Saudi Arabia'::text, 'United Arab Emirates'::text, 'Bahrain'::text, 'Kuwait'::text, 'Qatar'::text, 'Oman'::text])) 
		  AND (worldbank_crime_terror.metric = ANY (ARRAY['Intentional homicides (per 100,000 people)'::text, 'Crime (% of managers surveyed ranking this as a major constraint)'::text, 'Intentional homicides, female (per 100,000 female)'::text, 'Percent of firms choosing crime, theft and disorder as their biggest obstacle'::text, 'Safety and Rule of Law'::text, 'Intentional homicides, male (per 100,000 male)'::text, 'Political Stability/No Violence (estimate)'::text, 'Percent of firms identifying crime, theft and disorder as a major constraint'::text]))) sq
  WHERE sq.value IS NOT NULL AND sq.value <> 0::numeric;