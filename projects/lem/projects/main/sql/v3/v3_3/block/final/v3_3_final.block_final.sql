CREATE TABLE v3_3_final.block_final as 

------this is the majority of the counts 
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
  v3_3_main.block_main 
WHERE block_main.nwalt NOT IN (11,21,60) AND block_main.nwalt_rc IS NOT NULL

UNION

------ null all nwalt60 and nwalt11 records (note: might need to rerun script with different where clause condtion)
SELECT 
  block_main.geoid,
  block_main.block_group,
  block_main.hectares, 
  block_main.neighbor_list, 
  --block_main.nwalt,
  --block_main.nwalt_rc,
  NULL as luc,
  block_main.biomes,
  block_main.lng, 
  block_main.lat, 
  block_main.geom
FROM v3_3_main.block_main
WHERE block_main.nwalt IN (11,60)

UNION

-------all the nwalt 21 records that dont have to be interpoloated
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
  v3_3_main.block_main 
WHERE block_main.nwalt = 21 AND block_main.nwalt_rc IS NOT NULL

UNION

------records that are interpolated from neighbors row 34,18
SELECT * FROM v3_3_refine.block_main_nwalt_null_neighbor_values_mode_formatted



