
----county acres as denominator

SELECT 
  a.label, 
  a.atlas_stco, 
  a.acres,
  c.acres_calc,
  ROUND(((a.acres/c.acres_calc)*100)::numeric,0) as perc,
  ST_Transform(counties_102003.wkb_geometry,4326) as geom
FROM 
  intact_conversion.intact_conversion_11and15_hist_counties as a INNER JOIN 
  spatial.counties_102003 as c
ON
  c.atlas_stco = a.atlas_stco
WHERE label = '4' 
AND intact_conversion_11and15_hist_counties.atlas_stco NOT IN ('56013', '27119', '27111') 
AND ROUND(((intact_conversion_11and15_hist_counties.acres/counties_102003.acres_calc)*100)::numeric,0) > 0
ORDER BY (a.acres/c.acres_calc)*100 desc




----intactland 2011 acres as denominator

SELECT 
  a.label, 
  a.atlas_stco, 
  a.acres,
 -- b.acres,
  c.acres_calc,
  ROUND(((a.acres/b.acres)*100)::numeric,0) as perc,
  ST_Transform(counties_102003.wkb_geometry,4326) as geom
FROM 
  intact_conversion.intact_conversion_11and15_hist_counties as a INNER JOIN 
  spatial.counties_102003 as c
ON
  c.atlas_stco = a.atlas_stco
INNER JOIN
intact_conversion.intactland_11_refined_hist_counties as b
ON 
c.atlas_stco = b.atlas_stco
WHERE a.label = '4' AND b.label='11' AND b.acres <> 0
ORDER BY (a.acres/c.acres_calc)*100 desc
