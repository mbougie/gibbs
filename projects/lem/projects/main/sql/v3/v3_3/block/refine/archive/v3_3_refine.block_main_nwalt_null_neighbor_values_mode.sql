create table v3_3_qaqc.v3_3_block_main_nwalt_null_neighbor_values_mode_w_geom as 

SELECT 
  v3_3_block_main_nwalt_null_neighbor_values_mode.a_geoid, 
  v3_3_block_main_nwalt_null_neighbor_values_mode.b_geoid, 
  v3_3_block_main_nwalt_null_neighbor_values_mode.luc, 
  v3_3_block_main_nwalt_null_neighbor_values_mode.biomes, 
  block_main.geom
FROM 
  v3_3_refine.v3_3_block_main_nwalt_null_neighbor_values_mode, 
  v3_3.block_main
WHERE 
  block_main.geoid = v3_3_block_main_nwalt_null_neighbor_values_mode.a_geoid;
