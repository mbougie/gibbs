CREATE TABLE amanda.census_2018 as 

SELECT
state::text as state,
RIGHT('00'|| COALESCE(state::text,''),2) as state_mod,
county::text as county,
RIGHT('00'|| COALESCE(county::text,''),3) as county_mod,
year,
agegrp,
tot_pop,
RIGHT('00'|| COALESCE(state::text,''),2) || RIGHT('00'|| COALESCE(county::text,''),3) as fips
FROM 
amanda.census_2018_control;



