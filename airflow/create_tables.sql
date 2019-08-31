CREATE TABLE IF NOT EXISTS public.staging_capstone_events(
	region varchar(256),
	state varchar(256),
	city varchar(256),
	resale varchar(256),
	product varchar(256),
	registration_date varchar(256),
	sale_price numeric(4,3),
	purchase_price numeric(4,3),
	unit varchar(256),
	seller varchar(256)
);

CREATE TABLE IF NOT EXISTS public.staging_capstone_states (
	state varchar(256),
	state_name varchar(256),
	population varchar(256),
	per_capita_income varchar(256)
);

CREATE TABLE IF NOT EXISTS public.staging_capstone_region (
	region varchar(256),
	region_name varchar(256),
	area_km varchar(256)
);


CREATE TABLE IF NOT EXISTS public.capstone_events (
	region varchar(256) NOT NULL,
	state varchar(256) NOT NULL,
	city varchar(256),
	resale varchar(256),
	product varchar(256) NOT NULL,
	registration_date date NOT NULL,
	sale_price NUMERIC(4,3) NOT NULL,
	purchase_price NUMERIC(4,3),
	seller varchar(256)
);
CREATE TABLE IF NOT EXISTS public.capstone_states (
	state varchar(256) ,
	state_name varchar(256),
	population NUMERIC,
	per_capita_income NUMERIC,
	CONSTRAINT state_pkey PRIMARY KEY (state)
);

CREATE TABLE IF NOT EXISTS public.capstone_region (
	region varchar(256),
	region_name varchar(256),
	area_km NUMERIC,
	CONSTRAINT region_pkey PRIMARY KEY (region)
);

CREATE TABLE IF NOT EXISTS public.capstone_time(
	registration_date DATE PRIMARY KEY,
    day INTEGER, month INTEGER,
    year INTEGER, quarter INTEGER,
    week INTEGER);





-------
INSERT FATO

SELECT region, state, city, 
resale, product, TO_DATE(registration_date,'DD/MM/YYYY'),
sale_price,
purchase_price,
seller
FROM staging_capstone_events2;

---
INSERT DIM STATE

SELECT distinct state, state_name, population, per_capita_income
FROM staging_capstone_states;

---
INSERT DIM REGION

SELECT distinct region, region_name, area_km
FROM staging_capstone_region;
---
INSERT DIM TIME

SELECT registration_date,
EXTRACT(day from registration_date) as day,
EXTRACT(month from registration_date) as month,
EXTRACT(year from registration_date) as year,
EXTRACT(quarter from registration_date) as quarter,
EXTRACT(week from registration_date) as week
from capstone_events;




-----
QUERY DE DESENVOLVIMENTO

INSERT INTO capstone_events
SELECT region, state, city, 
resale, product, TO_DATE(registration_date,'DD/MM/YYYY') as registration_date,
sale_price,
purchase_price,
seller
FROM staging_capstone_events6;

