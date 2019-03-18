create table v1_wisc_qaqc.block_group_4471 as 

SELECT 
  block_group.geoid, 
  block_group.wkb_geometry
FROM 
    (SELECT 
  geoid 
FROM 
  v1_wisc.block_neighbors_cleaned
group by geoid) as neighbors, 
  v1_wisc.block_group
WHERE 
  block_group.geoid = neighbors.geoid;
