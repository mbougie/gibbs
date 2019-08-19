SELECT 
  states.st_abbrev, 
  states.atlas_name,
  sum(intactlands_union_pad.acres)
FROM 
  intact_lands.intactlands_union_pad, 
  spatial.counties, 
  spatial.states
WHERE 
  intactlands_union_pad.atlas_stco = counties.atlas_stco AND LEFT(counties.atlas_stco,2)=states.atlas_st
GROUP BY   
  states.st_abbrev, 
  states.atlas_name