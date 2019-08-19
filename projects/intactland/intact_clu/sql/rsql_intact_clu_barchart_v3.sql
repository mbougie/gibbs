-----------------------------------------------------
----intact lands (ALL lands protected and non-protected)
-----------------------------------------------------
SELECT 
b.st_abbrev, 
a.acres::integer as acres,
-----scale both intact acres
---(a.acres::integer)/100000 as acres,
----create new column and convert the values to text
CASE
when a.value = '1' then 'forest_p'
when a.value = '2' then 'wetland_p'
when a.value = '3' then 'grassland_p'
when a.value = '4' then 'shrubland_p'
END landcover
-----------------------------------------------------
FROM 
intact_clu.intactland_15_refined_cdl15_broad_hist_states as a 
INNER JOIN
spatial.states as b
USING(atlas_st)


UNION
-------------------------------------------------------------------
----NON-intact lands (ALL lands protected and non-protected)
-------------------------------------------------------------------
SELECT 
b.st_abbrev, 
a.acres::integer as acres,
---(a.acres::integer)/100000 as acres,
--c.pad_30m_b as pad,
c.grps as landcover

FROM 
intact_clu.combined_hist_states as a 
INNER JOIN
spatial.states as b
USING(atlas_st)
INNER JOIN
intact_clu.combined as c
ON a.value::integer = c.value

---don't want to work with intact lands
WHERE c.grps != 'intact' 


/*
-------------------------------------------------------------------
----BOTH intact and non-intact protected (This NEEDS to overlap the other data)
-------------------------------------------------------------------
UNION

Eventually want to add the pad for BOTH intact and non-intact lands (need to refine the second table in this script when I do this!!!!!!!!!!!!!!!!!!!!)
*/