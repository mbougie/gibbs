SELECT 
  zonal_hist_nwalt_rc_60m_state.index, 
  zonal_hist_nwalt_rc_60m_state.label, 
  zonal_hist_nwalt_rc_60m_state.atlas_st, 
  zonal_hist_nwalt_rc_60m_state.count, 
  zonal_hist_nwalt_rc_60m_state.value, 
  spatial.states_102003.name
FROM 
  v3_2_qaqc.zonal_hist_nwalt_rc_60m_state, 
  spatial.spatial.states_102003
WHERE 
  spatial.states_102003.statefp = zonal_hist_nwalt_rc_60m_state.atlas_st;




SELECT
  atlas_name, 
  sum(zonal_hist_nwalt_rc_60m_state.count),
  sum(zonal_hist_nwalt_rc_60m_state.count) * 0.8895794 as acres,
  sum(zonal_hist_nwalt_rc_60m_state.count) * 0.360000010812 as hectares,
  acres_calc,
  ROUND(((sum(zonal_hist_nwalt_rc_60m_state.count) * 0.8895794)/ acres_calc)::numeric * 100,2) as perc
FROM 
  v3_2_qaqc.zonal_hist_nwalt_rc_60m_state, 
  spatial.states_102003
WHERE 
  spatial.states_102003.atlas_st = zonal_hist_nwalt_rc_60m_state.atlas_st and value = '25'
GROUP BY atlas_name, acres_calc
order by ((sum(zonal_hist_nwalt_rc_60m_state.count) * 0.8895794)/ acres_calc) * 100 DESC