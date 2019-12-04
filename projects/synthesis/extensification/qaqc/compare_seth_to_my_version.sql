SELECT 
  agroibis_counties.fips, 
  agroibis_counties.lrr_group, 
  agroibis_counties.n_aban_imp_rfs, 
  "abd_diff_Nleach_fips_summary".atlas_stco, 
  "abd_diff_Nleach_fips_summary".mean,
  "abd_diff_Nleach_fips_summary".sd,
  agroibis_counties.n_aban_imp_rfs - "abd_diff_Nleach_fips_summary".mean as difference_of_means,
  acres_calc
  
FROM 
  extensification_agroibis.agroibis_counties FULL OUTER JOIN 
  extensification_seth."abd_diff_Nleach_fips_summary"
ON
  agroibis_counties.fips = "abd_diff_Nleach_fips_summary".atlas_stco
FULL OUTER JOIN spatial.counties USING(fips)
WHERE fips is not null and "abd_diff_Nleach_fips_summary".atlas_stco is null
ORDER BY agroibis_counties.n_aban_imp_rfs - "abd_diff_Nleach_fips_summary".mean



SELECT 
  sum(agroibis_counties.n_aban_imp_rfs) as n_aban_imp_rfs, 
  sum("abd_diff_Nleach_fips_summary".mean) as abd_diff_Nleach_fips_summary,
  (sum(agroibis_counties.n_aban_imp_rfs)) - (sum("abd_diff_Nleach_fips_summary".mean)) as difference_of_means,
  
FROM 
  extensification_agroibis.agroibis_counties FULL OUTER JOIN 
  extensification_seth."abd_diff_Nleach_fips_summary"
ON
  agroibis_counties.fips = "abd_diff_Nleach_fips_summary".atlas_stco
INNER JOIN spatial.counties USING(fips)
