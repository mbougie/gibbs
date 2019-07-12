---Query returned successfully: 215858 rows affected, 107659 ms execution time.

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

 ---exclude these records 
WHERE geoid NOT IN
	(SELECT 
	 geoid 
	FROM 
	  v3_2.block_v3_main
	---find a records that need to have a more granular zonal table from 10m res rasters
	WHERE 
	  nwalt_rc IS NULL  --these are null becasue the raster was null 
	  AND neighbor_list IS NOT NULL --- these are block group objects that are mostly islands
	  AND (nwalt IN (21) OR nwalt IS NULL)  --- want records that are nwalt value 21 (not 11 (water)) OR are completly null in all columns becasue of size
	)

UNION

----note only xxxxxx records here because nwalt_rc still couldnt put a value inside major transortation polygon i.e. it was all major transportation becasue even if a little of another class would have been in here it would have been labeled that class
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
  v3_2.block_v3_main,
  v3_2.block_zonal_maj_nwalt_rc_10m, 
  v3_2.block_zonal_maj_biomes_10m

WHERE 
  block_zonal_maj_nwalt_rc_10m.geoid = block_v3_main.geoid AND
  block_zonal_maj_biomes_10m.geoid = block_v3_main.geoid;



