/*
Description: create table that ONLY contains the records that are not nwalt raw classes:
	1 water
	2 conservation

Reasoning: Removed these classes because they are going to be converted to null or reclassed (i.e. major transportation < 2 hectare or null)
*/




----uery returned successfully: 9884558 rows affected, 879456 ms execution time.

CREATE TABLE v3_3_refine.block_main_core as 
SELECT 
  block_main.geoid, 
  block_main.block_group, 
  block_main.hectares, 
  block_main.neighbor_list, 
  block_main.nwalt_rc as luc,
  block_main.biomes as biomes,
  block_main.lng, 
  block_main.lat, 
  block_main.geom
FROM 
  v3_3.block_main 
WHERE block_main.nwalt NOT IN (11,60)


