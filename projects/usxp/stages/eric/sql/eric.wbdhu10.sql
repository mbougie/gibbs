create table eric.wbdhu10 as 

(SELECT 
points.gridcode, 
wbdhu10.huc10
FROM 
eric.s35_mtr3_4_id_pts_wgs84 as points, -- coordinates
spatial.wbdhu10  -- polygons

WHERE 
ST_Within(points.wkb_geometry, ST_Transform(wbdhu10.geom,4326)))