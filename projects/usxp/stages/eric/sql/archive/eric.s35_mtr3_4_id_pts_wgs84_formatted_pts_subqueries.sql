--create table eric.points_formatted_pts_extra as  

--EXPLAIN
SELECT 
  points.gridcode, 
  st_x((ST_Dump(points.wkb_geometry)).geom) as lon,
  st_y((ST_Dump(points.wkb_geometry)).geom) as lat, 
  points.s35_mtr as mtr, 
  points.s35_fc as fc, 
  points.s35_bfnc as bfnc,
  COALESCE(points.s35_fc, points.s35_bfnc) as fc_bfnc,
  points.s35_bfc as bfc, 
  points.s35_fnc as fnc,
  COALESCE(points.s35_bfc, points.s35_fnc) as bfc_fnc,
  lu_1.veg_type_ext as vegScen1, 
  lu_2.veg_type_ext as vegScen2,
  counties.atlas_stco,
  mlra.mlra_id
  -- mtr3_4_id_pts_wgs84.wkb_geometry as geom
FROM 
  eric_lookup.lookup as lu_1,
  eric.s35_mtr3_4_id_pts_wgs84 as points, -- coordinates
  eric_lookup.lookup as lu_2,

----counties---------------------------------------
(SELECT 
points.gridcode, 
counties.atlas_stco
FROM 
eric.points, -- coordinates
spatial.counties  -- polygons

WHERE 
ST_Within(points.wkb_geometry, ST_Transform(counties.geom,4326)) 
LIMIT 100) as counties, -- polygons


----mlra---------------------------------------
(SELECT 
points.gridcode, 
mlra.mlra_id
FROM 
eric.points, -- coordinates
spatial.mlra  -- polygons

WHERE 
ST_Within(points.wkb_geometry, ST_Transform(mlra.wkb_geometry,4326)) 
LIMIT 100) as mlra, -- polygons



----huc10---------------------------------------
(SELECT 
points.gridcode, 
wbdhu10.huc10
FROM 
eric.points, -- coordinates
spatial.wbdhu10  -- polygons

WHERE 
ST_Within(points.wkb_geometry, ST_Transform(wbdhu10.geom,4326)) 
LIMIT 100) as wbdhu10 -- polygons




WHERE 
   lu_1.cdl = COALESCE(points.s35_fc, points.s35_bfnc) AND
  lu_2.cdl = COALESCE(points.s35_bfc, points.s35_fnc) AND
  points.gridcode=counties.gridcode AND
 points.gridcode=mlra.gridcode
  --points.gridcode=wbdhu10.gridcode
  
  -- ST_Within(points.wkb_geometry, ST_Transform(wbdhu10.geom,4326))
limit 100





