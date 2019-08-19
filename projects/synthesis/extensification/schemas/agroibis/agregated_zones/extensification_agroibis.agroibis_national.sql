----extensification_agroibis.agroibis_national

SELECT 
  sum(agroibis_counties.p_exp_imp_rfs) p_exp_imp_rfs, 
  sum(agroibis_counties.p_aban_imp_rfs) p_aban_imp_rfs, 
  sum(agroibis_counties.n_exp_imp_rfs) n_exp_imp_rfs, 
  sum(agroibis_counties.n_aban_imp_rfs) n_aban_imp_rfs, 
  sum(agroibis_counties.sed_exp_imp_rfs) sed_exp_imp_rfs, 
  sum(agroibis_counties.sed_aban_imp_rfs) d_aban_imp_rfs
FROM 
  extensification_agroibis.agroibis_counties;
