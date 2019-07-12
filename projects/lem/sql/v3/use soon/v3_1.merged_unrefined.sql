create table v3_2.merged_unrefined as 

SELECT 
  block_v3_main.geoid, 
  block_v3_main.block_group, 
  block_v3_main.hectares, 
  block_v3_main.nwalt, 
  block_v3_main.nwalt_rc, 
  block_v3_main.biomes, 
  block_v3_neighbors.neighbor_list, 
  block_v3_main.lng, 
  block_v3_main.lat, 
  block_v3_main.geom
FROM 
  v3_2.block_v3_main, 
  v3_2.block_v3_neighbors
WHERE 
  block_v3_main.geoid = block_v3_neighbors.geoid;



----NOTE---Need to rerun this becasue indexing WRONG table i.e. v3_1!!!!!!!!!!!!!!!!
CREATE INDEX v3_2_merged_unrefined_geom_idx
  ON v3_2.merged_unrefined
  USING gist
  (geom);


