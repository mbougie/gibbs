----Query returned successfully: 215757 rows affected, 50725 ms execution time.
---purpose
---to get a neighborlist of each block_group geoid


---Note: 215,757 versus 215,834. The reasons for the difference is because these block group objects are islands w/ no neighbors adjacent to them.



CREATE TABLE v3_2.block_group_v3_neighbors AS 

-- derive neighbors column-----------------------------------------------------
SELECT 
a.src_geoid,
string_agg(a.nbr_geoid, ', '::text) AS neighbor_list,

FROM v3_2.block_group_neighbors a 
WHERE a.length <> 0::double precision 
GROUP BY a.src_geoid, b.geoid









-------------  qaqc query  ----------------------------------
/*
CREATE TABLE v3_2.block_group_v3_neighbors_qaqc AS 

-- derive neighbors column-----------------------------------------------------
SELECT 
a.src_geoid,
string_agg(a.nbr_geoid, ', '::text) AS neighbor_list,
b.geoid

FROM v3_2.block_group_neighbors a full outer join v3_2.block_group as b ON a.src_geoid = b.geoid
--WHERE a.length <> 0::double precision 
GROUP BY a.src_geoid, b.geoid
*/