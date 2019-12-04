--Description: This is a table that containing records that will be sent back to arcgis so majority stats can be calulated using a finer spatial resoution rwalt_rc and biomes rasters
----Query returned successfully: 1175177 rows affected, 120420 ms execution time.


----NOTE this is a bad query because reclassifying large major transportation areas like airports
CREATE table v3_3.block_replace_nwalt21 as 

SELECT 
 * 
FROM 
  v3_3.block_main

---find a records that need to have a more granular zonal table from the 10m res rasters
WHERE 
  nwalt_rc IS NULL  --these are null becasue the raster was null 
  ---AND neighbor_list IS NOT NULL
  AND (nwalt IN (21) OR nwalt IS NULL);  --- want records that are nwalt value 21 (not 11 (water)) OR are completly null in all columns becasue of size


---create index using geoid column
CREATE INDEX v3_3_block_replace_nwalt21geoid_idx ON v3_3.block_replace_nwalt21 (geoid);