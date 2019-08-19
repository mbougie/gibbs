----Query returned successfully: 11007989 rows affected, 1590061 ms execution time.

CREATE TABLE v3_2.block_final as 

--EXPLAIN

SELECT * FROM v3_2.block_final_core

UNION

SELECT * FROM v3_2.block_final_water

UNION

SELECT * FROM v3_2.block_final_bottom;



------create indexes after table is created-------------------------------------------
---create index using geoid column
CREATE INDEX v3_2_block_final_geoid_idx ON v3_2.block_final (geoid);

---create index using geom column
CREATE INDEX v3_2_block_final_geom_idx ON v3_2.block_final USING gist (geom);


