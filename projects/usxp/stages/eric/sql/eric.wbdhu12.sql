create table eric.wbdhu12 as 

(SELECT 
points.gridcode, 
wbdhu12.huc12
FROM 
eric.s35_mtr3_4_id_pts_wgs84 as points, -- coordinates
spatial.wbdhu12  -- polygons

WHERE 
ST_Within(points.wkb_geometry, ST_Transform(wbdhu12.geom,4326)))