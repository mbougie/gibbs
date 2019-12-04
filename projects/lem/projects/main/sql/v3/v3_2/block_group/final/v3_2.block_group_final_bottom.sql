----Query returned successfully: 26 rows affected, 63 ms execution time.

CREATE TABLE v3_2.block_group_final_bottom as 
SELECT 
  block_group_replace.geoid, 
  block_group_replace.hectares, 
  block_group_replace.neighbor_list, 
  block_group_zonal_maj_nwalt_rc_10m.majority as luc,
  block_group_zonal_maj_biomes_10m.majority as biomes, 
  block_group_replace.lng, 
  block_group_replace.lat, 
  block_group_replace.geom

FROM 
  v3_2.block_group_replace 
  LEFT OUTER JOIN
  v3_2.block_group_zonal_maj_nwalt_rc_10m ON block_group_zonal_maj_nwalt_rc_10m.geoid = block_group_replace.geoid 
   
  LEFT OUTER JOIN 
  v3_2.block_group_zonal_maj_biomes_10m ON block_group_zonal_maj_biomes_10m.geoid = block_group_replace.geoid




