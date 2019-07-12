
CREATE TABLE v3_2.block_main_final as 

--EXPLAIN

SELECT 
  block_v3_main.geoid, 
  block_v3_main.block_group, 
  block_v3_main.hectares, 
  block_v3_main.neighbor_list, 
  nwalt_rc as luc,
  biomes,
  block_v3_main.lng, 
  block_v3_main.lat, 
  block_v3_main.geom

FROM 
  v3_2.block_v3_main

 ---exclude these records because they are going to be replaced below
WHERE geoid NOT IN
	(SELECT 
	 geoid 
	FROM 
	  v3_2.block_v3_main_bad

	)

UNION

----note only less records because nwalt_rc still could not put a value inside major transportation polygon i.e. it was all major transportation because even if a little of another class would have been inside of polygon it would have been labeled that class.
SELECT 
  block_v3_main.geoid, 
  block_v3_main.block_group, 
  block_v3_main.hectares, 
  block_v3_main.neighbor_list, 
  block_zonal_maj_nwalt_rc_10m.majority as luc,
  block_zonal_maj_biomes_10m.majority, 
  block_v3_main.lng, 
  block_v3_main.lat, 
  block_v3_main.geom

FROM 
  v3_2.block_v3_main 
  LEFT OUTER JOIN
  v3_2.block_zonal_maj_nwalt_rc_10m ON block_zonal_maj_nwalt_rc_10m.geoid = block_v3_main.geoid 
   
  LEFT OUTER JOIN 
  v3_2.block_zonal_maj_biomes_10m ON block_zonal_maj_biomes_10m.geoid = block_v3_main.geoid




