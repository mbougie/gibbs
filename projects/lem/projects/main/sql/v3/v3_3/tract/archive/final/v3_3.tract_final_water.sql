----Query returned successfully: 291649 rows affected, 171097 ms execution time.
----Query returned successfully: 291649 rows affected, 422394 ms execution time.
----Query returned successfully: 291649 rows affected, 840365 ms execution time.

-----Description: create table that contains the records with majority 11 (water) and label these records null using the look up table
CREATE TABLE v3_3.block_final_water as  

SELECT 
  block_main.geoid, 
  block_main.block_group, 
  block_main.hectares, 
  block_main.neighbor_list, 
  --block_main.nwalt,
  --block_main.nwalt_rc,
  nwalt_lookup.reclass_v2 as luc,
  block_main.biomes,
  block_main.lng, 
  block_main.lat, 
  block_main.geom
FROM v3_3.block_main INNER JOIN public.nwalt_lookup
ON block_main.nwalt = nwalt_lookup.initial  ----need to reference the lookup table to reclassify the raw nwalt values to the grouped values!
 
WHERE block_main.nwalt = 11
