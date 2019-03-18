create table eric.counties as 

(SELECT 
points.gridcode, 
counties.atlas_stco
FROM 
eric.s35_mtr3_4_id_pts_wgs84 as points, -- coordinates
spatial.counties  -- polygons

WHERE 
ST_Within(points.wkb_geometry, ST_Transform(counties.geom,4326)))