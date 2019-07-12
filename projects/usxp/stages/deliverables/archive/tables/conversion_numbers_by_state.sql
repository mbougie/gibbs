---expansion/abandonment per state

SELECT
expand.atlas_st,
states.atlas_name as state,
states.acres_calc as acres_state,
expand.count as expand_count,
expand.acres as expand_acres,
abandon.count as abandon_count,
abandon.acres as abandon_acres,
(expand.count - abandon.count) as net_count,
(expand.acres - abandon.acres) as net_acres,
((expand.count - abandon.count) * 0.222395) net_acres_qaqc,
((expand.acres - abandon.acres)/states.acres_calc) as ratio_state,
((expand.acres - abandon.acres)/states.acres_calc)*100 as perc_state

FROM 
(SELECT 
  atlas_st,
  count,
  acres,
  value
FROM 
  zonal_hist.s35_zonal_hist_table_mtr
WHERE value = '3') as expand

INNER JOIN

(SELECT 
  atlas_st,
  count,
  acres,
  value
FROM 
  zonal_hist.s35_zonal_hist_table_mtr
WHERE value = '4') abandon

USING(atlas_st)

INNER JOIN

spatial.states USING(atlas_st)