-----------------------------------------------------
----intact lands (ALL lands protected and non-protected)
-----------------------------------------------------
SELECT 
b.st_abbrev, 
a.acres::integer as acres,
c.label
-----------------------------------------------------
FROM 
intact_clu.intactland_15_refined_cdl15_broad_pad_hist_states as a 

INNER JOIN
spatial.states as b
USING(atlas_st)

INNER JOIN
intact_clu.intactland_15_refined_cdl15_broad_pad as c
ON a.value::integer = c.value


UNION
-------------------------------------------------------------------
----NON-intact lands (ALL lands protected and non-protected)
-------------------------------------------------------------------
SELECT 
b.st_abbrev, 
sum(a.acres::integer) as acres,
c.label

FROM 
intact_clu.combined_hist_states as a 
INNER JOIN
spatial.states as b
USING(atlas_st)
INNER JOIN
intact_clu.combined as c
ON a.value::integer = c.value

---don't want to work with intact lands
WHERE c.label not in ('intact_p', 'intact_np')

GROUP BY b.st_abbrev, c.label 

---ORDER BY b.st_abbrev 
/*
-------------------------------------------------------------------
----BOTH intact and non-intact protected (This NEEDS to overlap the other data)
-------------------------------------------------------------------
UNION

Eventually want to add the pad for BOTH intact and non-intact lands (need to refine the second table in this script when I do this!!!!!!!!!!!!!!!!!!!!)
*/