--------counties---------------------------------------------
--NOTE: pk on subregion column atlas_stco so don't have to use GROUP BY clause
SELECT 
  atlas_stco as subregion,
  --'counties' as region,
  --counties.acres_calc, 
  st_area(st_transform(geom,5070)) * 0.000247105 as acres
FROM 
  spatial.counties
ORDER BY atlas_stco
--limit 100


--------mlra---------------------------------------------
--NOTE: pk conflict on subregion column mlra_id so need to use GROUP BY clause
SELECT
  mlra_id as subregion,
  --'mlra' as region,  
  SUM(st_area(st_transform(wkb_geometry,5070)) * 0.000247105) as acres
FROM 
  spatial.mlra
GROUP BY 
  mlra_id
ORDER BY mlra_id
--limit 100


--------wbdhu10---------------------------------------------
--NOTE: pk on subregion column huc10 so don't have to use GROUP BY clause
SELECT 
   huc10 as subregion,
  --'wbdhu10' as region,
  --areaacres,
  st_area(st_transform(geom,5070)) * 0.000247105 as acres
FROM 
  spatial.wbdhu10
ORDER BY huc10
--limit 100


--------wbdhu12---------------------------------------------
--NOTE: pk conflict on subregion column huc12 so need to use GROUP BY clause
SELECT 
  huc12 as subregion,
  --'wbdhu12' as region,
  --areaacres,
  SUM(st_area(st_transform(geom,5070)) * 0.000247105) as acres
  --count(huc12)  --qaqc
FROM 
  spatial.wbdhu12
GROUP BY
  huc12
ORDER BY huc12
--ORDER BY count(huc12) desc --qaqc
--limit 100