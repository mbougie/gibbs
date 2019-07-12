create table new.abandon_cdl_counties as 


SELECT
  'grassland' as landcover, 
  sum(main.acres) as acres,
  counties.atlas_name,counties.stco

FROM 
  new.zonal_hist_formatted as main
INNER JOIN spatial.counties ON counties.atlas_stco = main.atlas_stco
WHERE main.value IN (37,62,171,176,181)
GROUP BY
counties.atlas_name,counties.stco

UNION

SELECT 
  'forest' as landcover, 
  sum(main.acres) as acres,
  counties.atlas_name,counties.stco
FROM 
  new.zonal_hist_formatted as main
INNER JOIN spatial.counties ON counties.atlas_stco = main.atlas_stco
WHERE main.value IN (63,141,142,143)
GROUP BY
counties.atlas_name,counties.stco

UNION

SELECT 
  'wetland' as landcover, 
  sum(main.acres) as acres,
  counties.atlas_name,counties.stco
FROM 
  new.zonal_hist_formatted as main
INNER JOIN spatial.counties ON counties.atlas_stco = main.atlas_stco
WHERE main.value IN (83,87,190,195)
GROUP BY
counties.atlas_name,counties.stco

UNION

SELECT 
  'shrubland' as landcover, 
  sum(main.acres) as acres,
  counties.atlas_name,counties.stco
FROM 
  new.zonal_hist_formatted as main
INNER JOIN spatial.counties ON counties.atlas_stco = main.atlas_stco
WHERE main.value IN (64,65,131,152)
GROUP BY
counties.atlas_name,counties.stco
/*
UNION

SELECT 
  'other' as landcover, 
  sum(main.acres) as acres,
  counties.atlas_name,counties.stco
FROM 
  new.zonal_hist_formatted as main
INNER JOIN spatial.counties ON counties.atlas_stco = main.atlas_stco
WHERE main.value IN (81,82,88,92,111,112,121,122,123,124)
GROUP BY
counties.atlas_name,counties.stco

UNION

SELECT 
  'disturbed' as landcover, 
  sum(main.acres) as acres,
  counties.atlas_name,counties.stco
FROM 
  new.zonal_hist_formatted as main
INNER JOIN spatial.counties ON counties.atlas_stco = main.atlas_stco
WHERE main.value NOT IN (37,62,63,64,65,81,82,83,87,88,92,111,112,121,122,123,124,131,141,142,143,152,171,181,176,190,195)
GROUP BY
counties.atlas_name,counties.stco
*/










/*
-----QAQC------------------
SELECT 
  'other' as landcover, 
  label,
  acres,
  counties.atlas_name,counties.stco
FROM 
  new.zonal_hist_formatted as main
INNER JOIN spatial.counties ON counties.atlas_stco = main.atlas_stco
WHERE main.value IN (81,82,88,92,111,112,121,122,123,124)
*/








/*
SELECT 
  label,
  acres,
  atlas_st
FROM 
  new.zonal_hist_formatted as main
INNER JOIN spatial.counties ON counties.atlas_stco = main.atlas_stco
WHERE main.value NOT IN (37,62,63,64,65,81,82,83,87,88,92,111,112,121,122,123,124,131,141,142,143,152,171,181,176,190,195)
*/






