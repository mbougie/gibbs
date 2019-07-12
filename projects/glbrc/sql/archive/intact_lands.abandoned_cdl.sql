create table new.abandon_cdl as 


SELECT
  'grassland' as landcover, 
  sum(main.acres) as acres,
  states.atlas_name

FROM 
  new.zonal_hist as main
INNER JOIN spatial.states ON states.atlas_st = LEFT(main.atlas_stco,2)
WHERE index IN (37,62,171,176,181)
GROUP BY
states.atlas_name

UNION

SELECT 
  'forest' as landcover, 
  sum(main.acres) as acres,
  states.atlas_name
FROM 
  new.zonal_hist as main
INNER JOIN spatial.states ON states.atlas_st = LEFT(main.atlas_stco,2)
WHERE index IN (63,141,142,143)
GROUP BY
states.atlas_name

UNION

SELECT 
  'wetland' as landcover, 
  sum(main.acres) as acres,
  states.atlas_name
FROM 
  new.zonal_hist as main
INNER JOIN spatial.states ON states.atlas_st = LEFT(main.atlas_stco,2)
WHERE index IN (83,87,190,195)
GROUP BY
states.atlas_name

UNION

SELECT 
  'shrubland' as landcover, 
  sum(main.acres) as acres,
  states.atlas_name
FROM 
  new.zonal_hist as main
INNER JOIN spatial.states ON states.atlas_st = LEFT(main.atlas_stco,2)
WHERE index IN (64,65,131,152)
GROUP BY
states.atlas_name

UNION

SELECT 
  'other' as landcover, 
  sum(main.acres) as acres,
  states.atlas_name
FROM 
  new.zonal_hist as main
INNER JOIN spatial.states ON states.atlas_st = LEFT(main.atlas_stco,2)
WHERE index IN (81,82,88,92,111,112,121,122,123,124)
GROUP BY
states.atlas_name

UNION

SELECT 
  'disturbed' as landcover, 
  sum(main.acres) as acres,
  states.atlas_name
FROM 
  new.zonal_hist as main
INNER JOIN spatial.states ON states.atlas_st = LEFT(main.atlas_stco,2)
WHERE index NOT IN (37,62,63,64,65,81,82,83,87,88,92,111,112,121,122,123,124,131,141,142,143,152,171,181,176,190,195)
GROUP BY
states.atlas_name












-----QAQC------------------
SELECT 
  'other' as landcover, 
  label,
  acres,
  states.atlas_name
FROM 
  new.zonal_hist as main
INNER JOIN spatial.states ON states.atlas_st = LEFT(main.atlas_stco,2)
WHERE index IN (81,82,88,92,111,112,121,122,123,124)









/*
SELECT 
  label,
  acres,
  atlas_st
FROM 
  new.zonal_hist as main
INNER JOIN spatial.states ON states.atlas_st = LEFT(main.atlas_stco,2)
WHERE index NOT IN (37,62,63,64,65,81,82,83,87,88,92,111,112,121,122,123,124,131,141,142,143,152,171,181,176,190,195)
*/






