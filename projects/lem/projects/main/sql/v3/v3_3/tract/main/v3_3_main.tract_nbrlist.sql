




CREATE TABLE v3_3_main.tract_nbrlist AS 

-- derive neighbors column-----------------------------------------------------
SELECT 
a.src_geoid as geoid,
string_agg(a.nbr_geoid, ', '::text) AS neighbor_list

FROM v3_core.block_neighbors a 
WHERE a.length <> 0::double precision 
GROUP BY a.src_geoid;


---create index using geoid column
CREATE INDEX v3_3_main_block_nbrlist_geoid_idx ON v3_3_main.block_nbrlist (geoid);

