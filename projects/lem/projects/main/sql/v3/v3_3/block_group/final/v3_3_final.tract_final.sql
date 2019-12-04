-----Description: create table that contains the records with majority 11 (water) and label these records null using the look up table
CREATE TABLE v3_3_final.block_group_final as 
SELECT 
  block_group_main.geoid,
  block_group_main.tract,
  block_group_main.hectares, 
  block_group_main.neighbor_list, 
  block_group_main.nwalt_rc as luc,
  block_group_main.biomes as biomes,
  block_group_main.lng, 
  block_group_main.lat, 
  block_group_main.geom
FROM 
  v3_3_main.block_group_main 
WHERE block_group_main.nwalt NOT IN (11,21,60) AND block_group_main.nwalt_rc IS NOT NULL

UNION

SELECT 
  block_group_main.geoid,
  block_group_main.tract,
  block_group_main.hectares, 
  block_group_main.neighbor_list, 
  block_group_main.nwalt_rc as luc,
  block_group_main.biomes as biomes,
  block_group_main.lng, 
  block_group_main.lat, 
  block_group_main.geom
FROM 
  v3_3_main.block_group_main 
WHERE block_group_main.nwalt = 21 AND block_group_main.nwalt_rc IS NOT NULL

UNION

SELECT 
  block_group_main.geoid,
  block_group_main.tract,
  block_group_main.hectares, 
  block_group_main.neighbor_list, 
  --block_group_main.nwalt,
  --block_group_main.nwalt_rc,
  NULL as luc,
  block_group_main.biomes,
  block_group_main.lng, 
  block_group_main.lat, 
  block_group_main.geom
FROM v3_3_main.block_group_main

WHERE block_group_main.nwalt IN (11,60) OR nwalt IS NULL
--WHERE block_group_main.nwalt IN (11,60)


UNION
SELECT * FROM v3_3_refine.block_group_main_nwalt_null_neighbor_values_mode_formatted



