----Query returned successfully: 18686 rows affected, 848953 ms execution time.

-----Description: create table that contains the records with majority 60 (Very Low Use, Conservation) and label these records null using the look up table
CREATE TABLE v3_3.block_group_final_conservation as  

SELECT 
  block_group_main.geoid, 
  block_group_main.tract, 
  block_group_main.hectares, 
  block_group_main.neighbor_list, 
  nwalt_lookup.reclass_v2 as luc,
  block_group_main.biomes,
  block_group_main.lng, 
  block_group_main.lat, 
  block_group_main.geom
FROM v3_3.block_group_main INNER JOIN public.nwalt_lookup
ON block_group_main.nwalt = nwalt_lookup.initial  ----need to reference the lookup table to reclassify the raw nwalt values to the reclassed values !
 
WHERE block_group_main.nwalt = 60
