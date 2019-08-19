SELECT 
  a.label, 
  a.acres, 
  b.atlas_name, 
  b.acres_calc,
  c.atlas_name,
  ROUND(((a.acres/b.acres_calc)*100)::numeric,0) as perc,
  ST_Transform(b.geom,4326) as geom
FROM 
  intact_clu.intactland_15_refined_hist_counties as a INNER JOIN
  spatial.counties as b
ON
  b.atlas_stco = a.atlas_stco
 INNER JOIN spatial.states as c
ON 
 c.atlas_st = LEFT(b.atlas_stco,2)
 WHERE label = '15'
ORDER by ROUND(((a.acres/b.acres_calc)*100)::numeric,0) DESC

