

-------------  create the road buffer by couty --------------------------------------------------- 


-- Clip all lines (roads) by counties (here we assume counties geom are POLYGON or MULTIPOLYGONS)
-- NOTE: we are only keeping intersections that result in a LINESTRING or MULTILINESTRING because we don't
-- care about roads that just share a point
-- the dump is needed to expand a geometry collection into individual single MULT* parts
-- the below is fairly generic and will work for polys, etc. by just changing the where clause


-- Iowa: Query returned successfully: 278729 rows affected, 2101314 ms execution time.
-- Minnesota: Query returned successfully: 459506 rows affected, 4924220 ms execution time.
-- North Dakota: Query returned successfully: 259698 rows affected, 1153307 ms execution time.



-- South Dakota: Query returned successfully: 112485 rows affected, 794728 ms execution time.
 

create table refine_rasters.roads_buff25_southdakota_no_dissolve as 

SELECT clipped.atlas_name, ST_Buffer(ST_SnapToGrid(clipped_geom,0.0001),25) as geom
FROM 
(SELECT counties.atlas_name, (ST_Dump(ST_Intersection(counties.wkb_geometry, roads.wkb_geometry))).geom As clipped_geom
FROM spatial.states_102003 as counties
INNER JOIN refine.region_roads_102003 as roads
ON ST_Intersects(counties.wkb_geometry, roads.wkb_geometry)
)  As clipped

WHERE ST_Dimension(clipped.clipped_geom) = 1 and clipped.atlas_name = 'South Dakota';



-----create spatial index-----------------
CREATE INDEX roads_buff25_southdakota_no_dissolve_geom_idx
  ON refine_rasters.roads_buff25_southdakota_no_dissolve 
  USING gist
  (geom);


ALTER TABLE refine_rasters.roads_buff25_South Dakota_no_dissolve ALTER COLUMN geom TYPE geometry(MultiLineString,102003) USING ST_SetSRID(geom,102003);



-----dissolve dataset
create table refine_rasters.roads_buff25_southdakota_dissolved as 
SELECT atlas_name, st_union(geom) 
FROM refine_rasters.roads_buff25_southdakota_no_dissolve
group by atlas_name




ALTER TABLE refine_rasters.roads_buff25_South Dakota_no_dissolve ALTER COLUMN geom TYPE geometry(MultiLineString,102003) USING ST_SetSRID(geom,102003);


---------- erase layer by buffer -----------------------------------------------------------------------
/*
create table refine_rasters.erase_19073 as 
SELECT ST_Difference(a.wkb_geometry, b.geom) as geom
FROM spatial.states_102003 as a, refine_rasters.clip_19073 as b
WHERE a.wkb_geometry && b.geom AND a.atlas_name = '19073'