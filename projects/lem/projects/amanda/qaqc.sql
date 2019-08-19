----QAQC1: check to see if the pixel count conversion to acres matches county acres

SELECT
main.geoid,
main.hectares,
checker.hectares,
main.hectares - checker.hectares as difference_of_hectares

FROM

(SELECT 
  main.geoid, 
  sum(main.hectares) as hectares
FROM 
  amanda.main
---where geoid = '06091'
group by geoid) as main

INNER JOIN

(SELECT 
  county_conus.geoid, 
  st_area(county_conus.wkb_geometry) * 0.0001 as hectares
FROM 
  spatial.county_conus) as checker 
---WHERE geoid = '06091') as checker

USING(geoid)



----QAQC1: check to see if the pixel count conversion to acres matches county acres