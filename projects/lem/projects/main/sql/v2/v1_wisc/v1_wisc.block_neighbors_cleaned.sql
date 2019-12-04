create table v1_wisc.block_neighbors_cleaned as  


SELECT 
  block_neighbors.index, 
  block_neighbors.objectid, 
  block_neighbors.src_geoid, 
  block_neighbors.nbr_geoid, 
  block_neighbors.area, 
  block_neighbors.length, 
  block_neighbors.node_count, 
  block_group.geoid
FROM 
  v1_wisc.block_neighbors, 
  v1_wisc.block_group
WHERE 
  block_group.geoid = left(block_neighbors.src_geoid,12) AND
  block_group.geoid = left(block_neighbors.nbr_geoid,12)
