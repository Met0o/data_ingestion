-- external_data.knoema_middle_east_data_full source

CREATE OR REPLACE VIEW external_data.knoema_middle_east_data_full
AS WITH knoema_middle_east_data AS (
         SELECT 'burglary'::text AS table_name,
            knoema_burglary.country_code,
            knoema_burglary.location AS country,
            knoema_burglary.variable,
            knoema_burglary.date,
            round(knoema_burglary.value, 0) AS value
           FROM external_data.knoema_burglary
          WHERE (knoema_burglary.location = ANY (ARRAY['Saudi Arabia'::text, 'United Arab Emirates'::text, 'Bahrain'::text, 'Kuwait'::text, 'Qatar'::text, 'Oman'::text])) 
		  AND knoema_burglary.value <> 'NaN'::numeric 
		  AND (knoema_burglary.variable = ANY (ARRAY['Burglary Count'::text, 'Burglary Rate'::text, 'Homicide rate'::text, 'Incidence of corruption'::text, 'Organized crime'::text, 'Property rights(EOSQ051)'::text, 'Property rights(GCI4.A.01.06)'::text, 'Reliability of police services'::text, 'Security'::text, 'Stability'::text, 'Terrorism incidence'::text, 'Theft of Private Cars Count'::text, 'Theft of Private Cars Rate'::text]))
        UNION ALL
         SELECT 'crime'::text AS table_name,
            knoema_crime.country_code,
            knoema_crime.country,
            knoema_crime.indicator AS variable,
            knoema_crime.date,
            round(knoema_crime.value, 0) AS value
           FROM external_data.knoema_crime
          WHERE (knoema_crime.country = ANY (ARRAY['Saudi Arabia'::text, 'United Arab Emirates'::text, 'Bahrain'::text, 'Kuwait'::text, 'Qatar'::text, 'Oman'::text])) 
		  AND knoema_crime.value <> 'NaN'::numeric 
		  AND (knoema_crime.indicator = ANY (ARRAY['Burglary Count'::text, 'Burglary Rate'::text, 'Homicide rate'::text, 'Incidence of corruption'::text, 'Organized crime'::text, 'Property rights(EOSQ051)'::text, 'Property rights(GCI4.A.01.06)'::text, 'Reliability of police services'::text, 'Security'::text, 'Stability'::text, 'Terrorism incidence'::text, 'Theft of Private Cars Count'::text, 'Theft of Private Cars Rate'::text]))
        UNION ALL
         SELECT 'homicide'::text AS table_name,
            knoema_homicide.country_code,
            knoema_homicide.country,
            knoema_homicide.indicator AS variable,
            knoema_homicide.date,
            round(knoema_homicide.value, 0) AS value
           FROM external_data.knoema_homicide
          WHERE (knoema_homicide.country = ANY (ARRAY['Saudi Arabia'::text, 'United Arab Emirates'::text, 'Bahrain'::text, 'Kuwait'::text, 'Qatar'::text, 'Oman'::text])) 
		  AND knoema_homicide.value <> 'NaN'::numeric 
		  AND (knoema_homicide.indicator = ANY (ARRAY['Disappearances'::text, 'Disappearances, conflicts, and terrorism'::text, 'Freedom from Political Killings'::text, 'Freedom from Torture'::text, 'Homicide'::text, 'Human Freedom'::text, 'Legal System & Property Rights'::text, 'Organized Conflicts'::text, 'Protection of property rights'::text, 'Reliability of police'::text, 'Rule of Law'::text, 'Safety & Security'::text, 'Terrorism Fatalities'::text, 'Terrorism Injuries'::text, 'Violent Conflicts'::text]))
        ), all_data AS (
         SELECT DISTINCT ON (knoema_middle_east_data.table_name, knoema_middle_east_data.country_code, knoema_middle_east_data.country, knoema_middle_east_data.variable, knoema_middle_east_data.date) knoema_middle_east_data.table_name,
            knoema_middle_east_data.country_code,
            knoema_middle_east_data.country,
            knoema_middle_east_data.variable,
            knoema_middle_east_data.date,
            knoema_middle_east_data.value
           FROM knoema_middle_east_data
          ORDER BY knoema_middle_east_data.table_name, knoema_middle_east_data.country_code, knoema_middle_east_data.country, knoema_middle_east_data.variable, knoema_middle_east_data.date
        )
 SELECT all_data.table_name,
    all_data.country_code,
    all_data.country,
    all_data.variable,
    all_data.date,
    all_data.value
   FROM all_data;