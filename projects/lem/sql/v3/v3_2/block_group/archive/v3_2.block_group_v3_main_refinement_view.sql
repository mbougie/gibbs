CREATE VIEW v3_2.block_group_v3_main_refinement_view as 

SELECT 
 * 
FROM 
  v3_2.block_group_v3_main

---find a records that need to have a more granular zonal table from 10m res rasters
WHERE 
  nwalt_rc IS NULL  --these are null becasue the raster was null 
  AND neighbor_list IS NOT NULL --- these are block group objects that are mostly islands
  AND (nwalt IN (21) OR nwalt IS NULL)  --- want records that are nwalt value 21 (not 11 (water)) OR are completly null in all columns becasue of size


---qaqc these are block group objects that are mostly islands
--WHERE neighbor_list is null