SELECT 
  agroibis_counties.fips, 
  agroibis_counties.lrr_group, 
  agroibis_counties.n_exp_imp_rfs, 
  "ext_diff_Nleach_fips_summary".atlas_stco, 
  "ext_diff_Nleach_fips_summary".mean,
  "ext_diff_Nleach_fips_summary".sd,
  agroibis_counties.n_exp_imp_rfs - "ext_diff_Nleach_fips_summary".mean as difference_of_means,
  acres_calc
  
FROM 
  extensification_agroibis.agroibis_counties FULL OUTER JOIN 
  extensification_seth."ext_diff_Nleach_fips_summary"
ON
  agroibis_counties.fips = "ext_diff_Nleach_fips_summary".atlas_stco
FULL OUTER JOIN spatial.counties USING(fips)
WHERE acres_calc is not null
ORDER BY agroibis_counties.n_exp_imp_rfs - "ext_diff_Nleach_fips_summary".mean



SELECT 
  sum(agroibis_counties.n_exp_imp_rfs) as n_exp_imp_rfs, 
  sum("ext_diff_Nleach_fips_summary".mean) as ext_diff_Nleach_fips_summary,
  (sum(agroibis_counties.n_exp_imp_rfs)) - (sum("ext_diff_Nleach_fips_summary".mean)) as difference_of_means
  --sum(acres_calc) as acres_calc
FROM 
  extensification_agroibis.agroibis_counties FULL OUTER JOIN 
  extensification_seth."ext_diff_Nleach_fips_summary"
ON
  agroibis_counties.fips = "ext_diff_Nleach_fips_summary".atlas_stco
INNER JOIN spatial.counties USING(fips)
