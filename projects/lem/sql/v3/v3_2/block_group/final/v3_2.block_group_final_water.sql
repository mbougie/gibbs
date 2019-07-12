----Query returned successfully: 291649 rows affected, 171097 ms execution time.
----Query returned successfully: 291649 rows affected, 422394 ms execution time.

-----Description: create table that contains the records with majority 11 (water) and label these records null using the look up table
CREATE TABLE v3_2.block_group_final_water as  

SELECT 
  block_group_main.geoid, 
  block_group_main.hectares, 
  block_group_main.neighbor_list, 
  --block_group_main.nwalt,
  --block_group_main.nwalt_rc,
  nwalt_lookup.grouped as luc,
  block_group_main.biomes,
  block_group_main.lng, 
  block_group_main.lat, 
  block_group_main.geom
FROM v3_2.block_group_main INNER JOIN public.nwalt_lookup
ON block_group_main.nwalt = nwalt_lookup.initial  ----need to reference the lookup table to reclassify the raw nwalt values to the grouped values!
 
WHERE block_group_main.nwalt = 11
