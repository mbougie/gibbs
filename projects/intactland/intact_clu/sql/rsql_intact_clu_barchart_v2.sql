SELECT 
b.st_abbrev, 
a.acres::integer as acres,
-----scale both intact acres
(a.acres::integer)/100000 as acres_scale,
----create new column and convert the values to text
CASE
when a.value = '1' then 'forest'
when a.value = '2' then 'wetland'
when a.value = '3' then 'grassland'
when a.value = '4' then 'shrubland'
END landcover
-----------------------------------------------------
FROM 
intact_clu.intactland_15_refined_cdl15_broad_hist_states as a 
INNER JOIN
spatial.states as b
USING(atlas_st)


UNION


SELECT 
b.st_abbrev, 
a.acres::integer as acres,
-----scale both intact acres
(a.acres::integer)/100000 as acres_scale,
----create new column and convert the values to text
CASE
when a.value = '0' then 'non-intact'
END landcover
-----------------------------------------------------
FROM 
intact_clu.intactland_15_refined_hist_states as a 
INNER JOIN
spatial.states as b
USING(atlas_st)

---this selects only nonintact values from intact_clu.intactland_15_refined_hist_states
WHERE a.label='0'