
CREATE TABLE v3_2.block_final_union_top as 

--EXPLAIN

SELECT 
  block_main.geoid, 
  block_main.block_group, 
  block_main.hectares, 
  block_main.neighbor_list, 
  nwalt_rc as luc,
  biomes,
  block_main.lng, 
  block_main.lat, 
  block_main.geom

FROM 
  v3_2.block_main

 ---exclude these records because they are going to be replaced below
WHERE geoid NOT IN
	(SELECT 
	 geoid 
	FROM 
	  v3_2.block_main_replace

	)




