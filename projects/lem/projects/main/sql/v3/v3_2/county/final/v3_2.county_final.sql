----Query returned successfully: 215833 rows affected, 111079 ms execution time.

CREATE TABLE v3_2.county_final as 

--EXPLAIN

SELECT * FROM v3_2.county_final_core

UNION

SELECT * FROM v3_2.county_final_water;

------create indexes after table is created-------------------------------------------
---create index using geoid column
CREATE INDEX v3_2_county_final_geoid_idx ON v3_2.county_final (geoid);

---create index using geom column
CREATE INDEX v3_2_county_final_geom_idx ON v3_2.county_final USING gist (geom);




