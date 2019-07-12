--Query returned successfully: 362081 rows affected, 533876 ms execution time.
----description: there are records that are null in block_main nwalt_rc column do to water and dev being set to null in the raster.
----want to keep the water as null but want to fill dev with adjacent values
CREATE table v3_2.block_main_replace_v2 as 

SELECT 
 * 
FROM 
  v3_2.block_main

---want to fill all null value records that are NOT water (nwalt=11) 
---the records that want to fill are due to to nwalt = 21 being reclassed as or polygon size!
WHERE 
  nwalt_rc IS NULL AND nwalt NOT IN (11);  --- want records that are nwalt value 21 (not 11 (water)) OR are completly null in all columns becasue of size


---create index using geoid column
---CREATE INDEX v3_2_block_main_replace_geoid_idx ON v3_2.block_main_replace (geoid);