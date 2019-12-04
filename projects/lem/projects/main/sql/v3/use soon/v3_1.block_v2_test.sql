----this query finds all the block groups that contain blocks  ---Cannot have block child where no block_group parent-----------
----v3_1.block_group:215834
----this table: 215766

----Because there are more values in the block group than this table there CANNOT be a foreign key constreaint applied to block_group dataset.  Need to remove these block_group rows that don't coencide with 
----this dataset

---Questions:
---why are they different?
---what are the records that don't exist?


create table v3_1.qaqc_neighbors as 
SELECT 
---this gets ALL block groups in the USA
block_group.geoid,
block_group.wkb_geometry as geom

FROM 
---relation_1
v3_1.block_group, 

(---relation_2
----this subquery finds all the block_group parents that are refernced by block children   ----NOTE: not all bock_gorups in the US have children so they have to be removed.
SELECT 
left(block.geoid,12) as grouped_geoid
FROM 
v3_1.block
group by 
left(block.geoid,12)) as blocks
WHERE 
block_group.geoid = blocks.grouped_geoid;


---create index
CREATE INDEX v3_1_qaqc_neighbors_geom_idx
ON v3_1.qaqc_neighbors
USING gist
(geom);