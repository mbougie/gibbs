SELECT 
  zonal_hist_nwalt_60m_state.index, 
  zonal_hist_nwalt_60m_state.label, 
  zonal_hist_nwalt_60m_state.atlas_st, 
  zonal_hist_nwalt_60m_state.count, 
  zonal_hist_nwalt_60m_state.value, 
  states_102003.atlas_name
FROM 
  v3_2_qaqc.zonal_hist_nwalt_60m_state, 
  spatial.states_102003
WHERE 
  states_102003.st_abbrev = zonal_hist_nwalt_60m_state.atlas_st;




SELECT
  atlas_name,
  zonal_hist_nwalt_60m_state.label as raw_luc,
  sum(zonal_hist_nwalt_60m_state.count),
  sum(zonal_hist_nwalt_60m_state.count) * 0.8895794 as acres,
  sum(zonal_hist_nwalt_60m_state.count) * 0.360000010812 as hectares,
  acres_calc,
  ROUND(((sum(zonal_hist_nwalt_60m_state.count) * 0.8895794)/ acres_calc)::numeric * 100,2) as perc
FROM 
  v3_2_qaqc.zonal_hist_nwalt_60m_state, 
  spatial.states_102003
WHERE 
  states_102003.st_abbrev = zonal_hist_nwalt_60m_state.atlas_st and label IN ('31','32','33','50','60')
GROUP BY atlas_name, acres_calc, label
order by atlas_name, label