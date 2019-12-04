-----Query returned successfully: 9832812 rows affected, 885628 ms execution time.
CREATE TABLE v3_2.block_final_top_t2 as  

SELECT 
  block_main.geoid, 
  block_main.block_group, 
  block_main.hectares, 
  block_main.neighbor_list, 
  block_main.nwalt_rc as luc,
  block_main.biomes as biomes,
  block_main.lng, 
  block_main.lat, 
  block_main.geom
FROM 
  v3_2.block_main FULL OUTER JOIN v3_2.block_replace 
ON
  block_main.geoid = block_replace.geoid ----get only the non-replaced records (replaced records are merged later on with final_bottom to create the final dataset)

WHERE block_replace.geoid IS NULL   ----this condtion garrauntees that only the records that where not replaced are returned
AND block_main.nwalt <> 11
