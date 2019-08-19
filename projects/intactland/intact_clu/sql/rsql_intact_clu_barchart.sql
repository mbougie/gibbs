SELECT 
b.atlas_name, 
a.acres::integer as acres,
(acres::integer)/100000 as acres_scale, 
CASE
when value = '1' then 'forest'
when value = '2' then 'wetland'
when value = '3' then 'grassland'
when value = '4' then 'shrubland'
END landcover
FROM 
  intact_clu.intactland_15_refined_cdl15_broad_hist_states as a INNER JOIN
  spatial.states as b
ON
  a.atlas_st = b.atlas_st

