----Query returned successfully: 1175177 rows affected, 35429 ms execution time.
----Query returned successfully: 1175177 rows affected, 224183 ms execution time.

CREATE TABLE v3_2.block_final_bottom as 

----note only less records because nwalt_rc still could not put a value inside major transportation polygon i.e. it was all major transportation because even if a little of another class would have been inside of polygon it would have been labeled that class.
SELECT 
  block_replace.geoid, 
  block_replace.block_group, 
  block_replace.hectares, 
  block_replace.neighbor_list, 
  block_zonal_maj_nwalt_rc_10m.majority as luc,
  block_zonal_maj_biomes_10m.majority as biomes, 
  block_replace.lng, 
  block_replace.lat, 
  block_replace.geom

FROM 
  v3_2.block_replace 
  LEFT OUTER JOIN
  v3_2.block_zonal_maj_nwalt_rc_10m ON block_zonal_maj_nwalt_rc_10m.geoid = block_replace.geoid 
   
  LEFT OUTER JOIN 
  v3_2.block_zonal_maj_biomes_10m ON block_zonal_maj_biomes_10m.geoid = block_replace.geoid




