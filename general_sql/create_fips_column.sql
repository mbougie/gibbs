----description nice susinct functionality using RIGHT() and COALESCE together!!

SELECT
state::text as state,
RIGHT('00'|| COALESCE(state::text,''),2) as state_mod,
county::text as county,
RIGHT('00'|| COALESCE(county::text,''),3) as county_mod,
year,
agegrp,
RIGHT('00'|| COALESCE(state::text,''),2) || RIGHT('00'|| COALESCE(county::text,''),3) as fips
FROM 
amanda.census_2018_control;



