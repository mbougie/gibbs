----Query returned successfully: 18686 rows affected, 848953 ms execution time.

-----Description: create table that contains the records with majority 60 (Very Low Use, Conservation) and label these records null using the look up table
CREATE TABLE v3_3_refine.block_main_conservation as  

SELECT 
  block_main.geoid, 
  block_main.block_group, 
  block_main.hectares, 
  block_main.neighbor_list, 
  nwalt_lookup.reclass_v2 as luc,
  block_main.biomes,
  block_main.lng, 
  block_main.lat, 
  block_main.geom
FROM v3_3.block_main INNER JOIN public.nwalt_lookup
ON block_main.nwalt = nwalt_lookup.initial  ----need to reference the lookup table to reclassify the raw nwalt values to the reclassed values !
 
WHERE block_main.nwalt = 60
