-----Description: create table that contains the records with majority 11 (water) and label these records null using the look up table
CREATE TABLE v3_3_final.tract_final_t2 as 
SELECT 
  tract_main.geoid, 
  tract_main.hectares, 
  tract_main.neighbor_list, 
  tract_main.nwalt_rc as luc,
  tract_main.biomes as biomes,
  tract_main.lng, 
  tract_main.lat, 
  tract_main.geom
FROM 
  v3_3_main.tract_main 
WHERE tract_main.nwalt NOT IN (11,60)

UNION

SELECT 
  tract_main.geoid, 
  tract_main.hectares, 
  tract_main.neighbor_list, 
  --tract_main.nwalt,
  --tract_main.nwalt_rc,
  NULL as luc,
  tract_main.biomes,
  tract_main.lng, 
  tract_main.lat, 
  tract_main.geom
FROM v3_3_main.tract_main

WHERE tract_main.nwalt IN (11,60) OR nwalt IS NULL


