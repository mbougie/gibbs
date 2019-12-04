
--Query returned successfully: 215807 rows affected, 128887 ms execution time.
---Query returned successfully: 215808 rows affected, 97551 ms execution time.
---Query returned successfully: 215808 rows affected, 132406 ms execution time.

CREATE TABLE v3_2.block_group_final_top as  

SELECT 
  block_group_main.geoid, 
  block_group_main.hectares, 
  block_group_main.neighbor_list, 
  --block_group_main.nwalt as luc,
  nwalt_lookup.grouped as luc,
  block_group_main.biomes as biomes,
  block_group_main.lng, 
  block_group_main.lat, 
  block_group_main.geom
FROM 
  v3_2.block_group_main FULL OUTER JOIN
  v3_2.block_group_replace
ON
  block_group_main.geoid = block_group_replace.geoid  ----get only the non-replaced records (replaced records are merged later on with final_bottom to create the final dataset)
INNER JOIN public.nwalt_lookup
ON block_group_main.nwalt = nwalt_lookup.initial  ----need to reference the lookup table to reclassify the raw nwalt values to the grouped values!
 
WHERE block_group_replace.geoid IS NULL    ----this condtion garrauntees that only the records that where not replaced are returned


