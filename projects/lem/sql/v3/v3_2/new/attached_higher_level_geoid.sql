----Query returned successfully: 215280 rows affected, 8831297 ms execution time.
create table v3_2.block_group_final_t2 as 
SELECT 
  a.geoid, 
  b.geoid as tract_spatial,
  a.hectares, 
  a.neighbor_list, 
  a.luc, 
  a.biomes, 
  a.lng, 
  a.lat, 
  a.geom
FROM 
  v3_2.block_group_final as a, 
  v3_2.tract_final as b
WHERE a.geom && b.geom AND
     ST_WITHIN(a.geom, b.geom)
