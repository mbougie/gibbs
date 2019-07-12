--Query returned successfully with no result in 276360 ms.

CREATE table v3_2.block_group_replace as 

SELECT 
 * 
FROM 
  v3_2.block_group_main

---find a records that need to have a more granular zonal table from 10m res rasters
WHERE 
  nwalt_rc IS NULL  --these are null becasue the raster was null 
  AND neighbor_list IS NOT NULL
  AND (nwalt IN (21) OR nwalt IS NULL);  --- want records that are nwalt value 21 (not 11 (water)) OR are completly null in all columns becasue of size


---create index using geoid column
CREATE INDEX v3_2_block_group_replace_geoid_idx ON v3_2.block_group_replace (geoid);