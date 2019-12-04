----Query returned successfully: 1289834 rows affected, 22535 ms execution time.

CREATE TABLE v3_3.block_group_final_nwalt21_refined as 

----note only less records because nwalt_rc still could not put a value inside major transportation polygon i.e. it was all major transportation because even if a little of another class would have been inside of polygon it would have been labeled that class.
SELECT 
  main.geoid, 
  main.tract, 
  main.hectares, 
  main.neighbor_list, 
  block_group_zonal_maj_nwalt_rc_v2_10m.majority as luc,
  block_group_zonal_maj_biomes_10m.majority as biomes, 
  main.lng, 
  main.lat, 
  main.geom

FROM 
  v3_3.block_group_refine_nwalt21_and_null as main
  LEFT OUTER JOIN
  v3_3.block_group_zonal_maj_nwalt_rc_v2_10m USING(geoid)
   
  LEFT OUTER JOIN 
  v3_3.block_group_zonal_maj_biomes_10m USING(geoid)




