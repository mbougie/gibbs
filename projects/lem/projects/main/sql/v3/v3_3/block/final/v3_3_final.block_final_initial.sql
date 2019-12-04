-----Query returned successfully with no result in 3220436 ms.


CREATE TABLE v3_3_final.block_final as 

--EXPLAIN

SELECT * FROM v3_3_refine.block_main_core

UNION

SELECT * FROM v3_3_refine.block_main_water

UNION

SELECT * FROM v3_3_refine.block_main_nwalt_null_neighbor_values_mode_formatted

UNION

SELECT * FROM v3_3_refine.block_main_conservation;



------create indexes after table is created-------------------------------------------
---create index using geoid column
CREATE INDEX v3_3_final_block_final_geoid_idx ON v3_3_final.block_final (geoid);

---create index using geom column
CREATE INDEX v3_3_final_block_final_geom_idx ON v3_3_final.block_final USING gist (geom);




