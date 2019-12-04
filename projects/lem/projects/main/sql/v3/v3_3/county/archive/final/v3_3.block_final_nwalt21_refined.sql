----Query returned successfully: 1289834 rows affected, 22535 ms execution time.

CREATE TABLE v3_3.block_final_nwalt21_refined as 

----note only less records because nwalt_rc still could not put a value inside major transportation polygon i.e. it was all major transportation because even if a little of another class would have been inside of polygon it would have been labeled that class.
SELECT 
  block_replace_nwalt21.geoid, 
  block_replace_nwalt21.block_group, 
  block_replace_nwalt21.hectares, 
  block_replace_nwalt21.neighbor_list, 
  block_zonal_maj_nwalt_rc_v2_10m.majority as luc,
  block_zonal_maj_biomes_10m.majority as biomes, 
  block_replace_nwalt21.lng, 
  block_replace_nwalt21.lat, 
  block_replace_nwalt21.geom

FROM 
  v3_3.block_replace_nwalt21 
  LEFT OUTER JOIN
  v3_3.block_zonal_maj_nwalt_rc_v2_10m USING(geoid)
   
  LEFT OUTER JOIN 
  v3_3.block_zonal_maj_biomes_10m USING(geoid)




