----Query returned successfully: 215833 rows affected, 214501 ms execution time.

create table v3_2.block_group_final_t3 as 
SELECT 
  a.geoid, 
  LEFT(a.geoid,11) as tract,
  a.hectares, 
  a.neighbor_list, 
  a.luc, 
  a.biomes, 
  a.lng, 
  a.lat, 
  a.geom
FROM 
  v3_2.block_group_final as a
