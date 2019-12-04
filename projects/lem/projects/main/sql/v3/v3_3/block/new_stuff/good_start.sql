-----Query returned successfully: 813096 rows affected, 2138390 ms execution time.
-----Query returned successfully: 813096 rows affected, 3225107 ms execution time.  <----both columns
-----Query returned successfully: 813096 rows affected, 5264813 ms execution time.
create table v3_3_refine.v3_3_block_main_nwalt_null_neighbor_values as

SELECT 
  v3_3_block_main_nwalt_null.geoid, 
  ---v3_3_block_main_nwalt_null.neighbor_list, 
  ---v3_3_block_main_nwalt_null.nwalt, 
  ---v3_3_block_main_nwalt_null.nwalt_rc, 
  ---v3_3_block_main_nwalt_null.biomes, 
  ---block_neighbors.src_geoid, 
  ----block_neighbors.nbr_geoid, 
  block_main.nwalt, 
  block_main.nwalt_rc, 
  block_main.biomes
FROM 
  v3_3_qaqc.v3_3_block_main_nwalt_null, 
  v3_core.block_neighbors, 
  v3_3.block_main
WHERE 
  block_neighbors.src_geoid = v3_3_block_main_nwalt_null.geoid AND
  block_main.geoid = block_neighbors.nbr_geoid AND node_count = 0;



/*

SELECT 
  a.geoid, 
  ---v3_3_block_main_nwalt_null.neighbor_list, 
  ---v3_3_block_main_nwalt_null.nwalt, 
  ---v3_3_block_main_nwalt_null.nwalt_rc, 
  ---v3_3_block_main_nwalt_null.biomes, 
  ---block_neighbors.src_geoid, 
  ----block_neighbors.nbr_geoid, 
  max(block_main.nwalt) 
  --count(block_main.nwalt_rc), 
  --count(block_main.biomes)
FROM 
  v3_3_refine.v3_3_block_main_nwalt_null_neighbor_values as a, 
  v3_core.block_neighbors, 
  v3_3.block_main
WHERE 
  block_neighbors.src_geoid = a.geoid AND
  block_main.geoid = block_neighbors.nbr_geoid AND node_count = 0
GROUP BY a.geoid
ORDER BY a.geoid  
  --count(block_main.nwalt) DESC
--limit 1
*/



create table v3_3_refine.v3_3_block_main_nwalt_null_neighbor_values_mode_t2 as 

SELECT
a.geoid as luc_geoid,
a.mode as luc,
b.geoid as biomes_geoid,
b.mode as biomes
FROM
(SELECT 
a.geoid,
mode()
within group(order by block_main.nwalt_rc)
FROM 
  v3_3_refine.v3_3_block_main_nwalt_null_neighbor_values as a, 
  v3_core.block_neighbors, 
  v3_3.block_main
WHERE 
  block_neighbors.src_geoid = a.geoid AND
  block_main.geoid = block_neighbors.nbr_geoid AND node_count = 0
GROUP BY a.geoid
ORDER BY a.geoid) as a
  --count(block_main.nwalt) DESC
--limit 1
INNER JOIN 
(SELECT 
a.geoid,
mode()
within group(order by block_main.biomes)
FROM 
  v3_3_refine.v3_3_block_main_nwalt_null_neighbor_values as a, 
  v3_core.block_neighbors, 
  v3_3.block_main
WHERE 
  block_neighbors.src_geoid = a.geoid AND
  block_main.geoid = block_neighbors.nbr_geoid AND node_count = 0
GROUP BY a.geoid
ORDER BY a.geoid) as b
USING(geoid)