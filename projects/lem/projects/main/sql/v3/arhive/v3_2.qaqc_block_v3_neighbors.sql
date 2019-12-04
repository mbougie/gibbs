CREATE TABLE v3_2.qaqc_block_v3_neighbors AS 
SELECT 
    main.geoid,
    neighbors.neighbor_list,
    main.wkb_geometry as geom

FROM v3_2.block main

JOIN 

-- derive neighbors column-----------------------------------------------------
( SELECT a.src_geoid,
    string_agg(a.nbr_geoid, ', '::text) AS neighbor_list
   FROM v3_2.block_neighbors a
  WHERE a.length <> 0::double precision
  GROUP BY a.src_geoid) neighbors ON main.geoid = neighbors.src_geoid;


---create index using geoid column
CREATE INDEX v3_2_qaqc_block_v3_neighbors_geom_idx
  ON v3_2.qaqc_block_v3_neighbors
  USING gist
  (geom);