--ALTER TABLE external_data.country_data ADD PRIMARY KEY (id);
ALTER TABLE external_data.country_name_mapping ADD COLUMN id SERIAL; UPDATE external_data.country_name_mapping SET id = DEFAULT; ALTER TABLE external_data.country_name_mapping ADD PRIMARY KEY (id);

ALTER TABLE external_data.knoema_crime ADD COLUMN id SERIAL; UPDATE external_data.knoema_crime SET id = DEFAULT; ALTER TABLE external_data.knoema_crime ADD PRIMARY KEY (id);
ALTER TABLE external_data.knoema_homicide ADD COLUMN id SERIAL; UPDATE external_data.knoema_homicide SET id = DEFAULT; ALTER TABLE external_data.knoema_homicide ADD PRIMARY KEY (id);
ALTER TABLE external_data.knoema_burglary ADD COLUMN id SERIAL; UPDATE external_data.knoema_burglary SET id = DEFAULT; ALTER TABLE external_data.knoema_burglary ADD PRIMARY KEY (id);

ALTER TABLE external_data.unodc_corruption ADD COLUMN id SERIAL; UPDATE external_data.unodc_corruption SET id = DEFAULT; ALTER TABLE external_data.unodc_corruption ADD PRIMARY KEY (id);
ALTER TABLE external_data.unodc_homicide ADD COLUMN id SERIAL; UPDATE external_data.unodc_homicide SET id = DEFAULT; ALTER TABLE external_data.unodc_homicide ADD PRIMARY KEY (id);
ALTER TABLE external_data.unodc_violent_crime ADD COLUMN id SERIAL; UPDATE external_data.unodc_violent_crime SET id = DEFAULT; ALTER TABLE external_data.unodc_violent_crime ADD PRIMARY KEY (id);

ALTER TABLE external_data.global_terrorism_db ADD COLUMN id SERIAL; UPDATE external_data.global_terrorism_db SET id = DEFAULT; ALTER TABLE external_data.global_terrorism_db ADD PRIMARY KEY (id);
ALTER TABLE external_data.global_terrorism_index_2022 ADD COLUMN id SERIAL; UPDATE external_data.global_terrorism_index_2022 SET id = DEFAULT; ALTER TABLE external_data.global_terrorism_index_2022 ADD PRIMARY KEY (id);
ALTER TABLE external_data.global_terrorism_index_2023 ADD COLUMN id SERIAL; UPDATE external_data.global_terrorism_index_2023 SET id = DEFAULT; ALTER TABLE external_data.global_terrorism_index_2023 ADD PRIMARY KEY (id);

ALTER TABLE external_data.worldbank_crime_terror ADD COLUMN id SERIAL; UPDATE external_data.worldbank_crime_terror SET id = DEFAULT; ALTER TABLE external_data.worldbank_crime_terror ADD PRIMARY KEY (id);