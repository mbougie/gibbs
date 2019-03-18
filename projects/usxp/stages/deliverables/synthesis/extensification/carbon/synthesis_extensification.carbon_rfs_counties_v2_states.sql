create table synthesis_extensification.carbon_rfs_counties_v2_states as 

SELECT 
  states.atlas_st,
  states.atlas_name,
  --carbon_rfs_counties_v2.atlas_stco, 
  --carbon_rfs_counties_v2.e_count, 
  --carbon_rfs_counties_v2.e_area, 
  --carbon_rfs_counties_v2.e_mg_c, 
  sum(carbon_rfs_counties_v2.e_gigagrams_co2e) as e_gigagrams_co2e, 
  --carbon_rfs_counties_v2.s_count, 
  --carbon_rfs_counties_v2.s_area, 
  --carbon_rfs_counties_v2.s_mg_c, 
  sum(carbon_rfs_counties_v2.s_gigagrams_co2e)* -1 as s_gigagrams_co2e,

  (sum(carbon_rfs_counties_v2.e_gigagrams_co2e)) + (sum(carbon_rfs_counties_v2.s_gigagrams_co2e)* -1) as net_gigagrams_co2e 
FROM 
  synthesis_extensification.carbon_rfs_counties_v2, 
  spatial.states, 
  spatial.counties
WHERE 
  states.atlas_st = counties.atlas_st AND
  counties.atlas_stco = carbon_rfs_counties_v2.atlas_stco
GROUP BY
  states.atlas_st,
  states.atlas_name 