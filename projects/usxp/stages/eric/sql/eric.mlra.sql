create table eric.mlra as

(SELECT 
points.gridcode, 
mlra.mlra_id
FROM 
eric.s35_mtr3_4_id_pts_wgs84 as points, -- coordinates
spatial.mlra  -- polygons

WHERE 
ST_Within(points.wkb_geometry, ST_Transform(mlra.wkb_geometry,4326)))