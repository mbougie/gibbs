SELECT 
  atlas_st,
  sum(acres) 
FROM 
  intact_clu.intactland_15_refined_cdl15_broad_hist_states
group by atlas_st
order by sum(acres) DESC