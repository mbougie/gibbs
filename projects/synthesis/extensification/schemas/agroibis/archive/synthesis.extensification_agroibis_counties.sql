CREATE TABLE extensification_agroibis.agroibis_counties as 
SELECT 
  fips_modified.fips,
  extensification_mlra.lrr_group,

  
  ---phosophourous------------------------------------
  "pExpImpInt",
  "pExpImp", 
  "pExpImp"*extensification_mlra.ratio_expand_rfs_mtr3 as p_exp_imp_rfs,
  
  "pAbanImpInt", 
  "pAbanImp",
  "pAbanImp"*extensification_mlra.ratio_abandon_rfs_mtr4 as p_aban_imp_rfs,

  ---nitrogen--------------------------------------
  "nExpImpInt",
  "nExpImp",
  "nExpImp"*extensification_mlra.ratio_expand_rfs_mtr3 as n_exp_imp_rfs, 
  
  "nAbanImpInt", 
  "nAbanImp",
  "nAbanImp"*extensification_mlra.ratio_abandon_rfs_mtr4 as n_aban_imp_rfs,

  ------sed--------------------------------------------
  "sedExpImpInt", 
  "sedExpImp",
  "sedExpImp"*extensification_mlra.ratio_expand_rfs_mtr3 as sed_exp_imp_rfs,
  
  "sedAbanImpInt",   
  "sedAbanImp",
  "sedAbanImp"*extensification_mlra.ratio_abandon_rfs_mtr4 as sed_aban_imp_rfs,

  --- et ---------------------------------------------------
  "etExpImpInt",
  "etExpImp",
  
  ("etExpImp"*extensification_mlra.ratio_expand_rfs_mtr3) as et_exp_imp_rfs,
  --convert acre/ft to gals
  (("etExpImp"*extensification_mlra.ratio_expand_rfs_mtr3) * 325851)/1000000 as gal_et_exp_imp_rfs,

  "etAbanImpInt",
  "etAbanImp",

  ("etAbanImp"*extensification_mlra.ratio_abandon_rfs_mtr4) as et_aban_imp_rfs,
  --convert acre/ft to gals
  (("etAbanImp"*extensification_mlra.ratio_abandon_rfs_mtr4) * 325851)/1000000 as gal_et_aban_imp_rfs,
  
  ----------------------------------------------------------



  --- irr ---------------------------------------------------
  "irrExpImpInt",
  "irrExpImp",
  "irrExpImp"*extensification_mlra.ratio_expand_rfs_mtr3 as irr_exp_imp_rfs,
  --convert acre/ft to gals
  (("irrExpImp"*extensification_mlra.ratio_expand_rfs_mtr3) * 325851)/1000000 as gal_irr_exp_imp_rfs,

  "irrAbanImpInt",
  "irrAbanImp",
  "irrAbanImp"*extensification_mlra.ratio_abandon_rfs_mtr4 as irr_aban_imp_rfs,
  --convert acre/ft to gals
  (("irrAbanImp"*extensification_mlra.ratio_abandon_rfs_mtr4) * 325851)/1000000 as gal_irr_aban_imp_rfs,
  ----------------------------------------------------------

  
  extensification_mlra.ratio_expand_rfs_mtr3,
  extensification_mlra.ratio_abandon_rfs_mtr4,
  --counties.atlas_stco,
  counties.geom
FROM 
  extensification_agroibis.agroibis_20190505
INNER JOIN

--table to create a fips column that is string instead of numeric
(SELECT 
CONCAT('0',"FIPS"::text) as fips,
"FIPS"
FROM 
extensification_agroibis.agroibis_20190505
where length("FIPS"::text) = 4

UNION

SELECT 
"FIPS"::text as fips,
"FIPS"
FROM 
extensification_agroibis.agroibis_20190505
where length("FIPS"::text) = 5) as fips_modified

USING("FIPS")

--join to get the geometry column
INNER JOIN spatial.counties 
ON fips_modified.fips = counties.atlas_stco

--used as an interface table(mlra regions and counties)between synthesis.extensification_mlra lrr_groups and the fips values of synthesis.extensification_agroibis_20190505_control 
INNER JOIN extensification_mlra.extensification_county_regions
ON fips_modified.fips=extensification_county_regions.fips

INNER JOIN extensification_mlra.extensification_mlra
ON extensification_county_regions.lrr_group=extensification_mlra.lrr_group