----Query returned successfully: 291649 rows affected, 171097 ms execution time.
----Query returned successfully: 291649 rows affected, 422394 ms execution time.
----Query returned successfully: 291649 rows affected, 840365 ms execution time.

-----Description: create table that contains the records with majority 11 (water) and label these records null using the look up table
CREATE TABLE v3_3_final.county_final as 
SELECT 
  county_main.geoid, 
  county_main.hectares, 
  county_main.neighbor_list, 
  county_main.nwalt_rc as luc,
  county_main.biomes as biomes,
  county_main.lng, 
  county_main.lat, 
  county_main.geom
FROM 
  v3_3_main.county_main 
WHERE county_main.nwalt NOT IN (11,60)

UNION

SELECT 
  county_main.geoid, 
  county_main.hectares, 
  county_main.neighbor_list, 
  --county_main.nwalt,
  --county_main.nwalt_rc,
  NULL as luc,
  county_main.biomes,
  county_main.lng, 
  county_main.lat, 
  county_main.geom
FROM v3_3_main.county_main

WHERE county_main.nwalt = 11

UNION

SELECT 
  county_main.geoid, 
  county_main.hectares, 
  county_main.neighbor_list, 
  --county_main.nwalt,
  --county_main.nwalt_rc,
  NULL as luc,
  county_main.biomes,
  county_main.lng, 
  county_main.lat, 
  county_main.geom
FROM v3_3_main.county_main

WHERE county_main.nwalt = 60

