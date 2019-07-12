--CREATE TABLE synthesis.extensification_agroibis as 
SELECT 
  fips_modified.fips,
  extensification_mlra.lrr_group,
  "pExpImp", 
  "pExpImp"*perc_expand_rfs as p_exp_imp_rfs, 
  "pAbanImp",
  "pAbanImp"*perc_abandon_rfs as p_aban_imp_rfs,
  "nExpImp",
  "nExpImp"*perc_expand_rfs as n_exp_imp_rfs, 
  "nAbanImp",
  "nAbanImp"*perc_abandon_rfs as n_aban_imp_rfs, 
  "sedExpImp",
  "sedExpImp"*perc_expand_rfs as sed_exp_imp_rfs,  
  "sedAbanImp",
  "sedAbanImp"*perc_abandon_rfs as sed_aban_imp_rfs, 
  perc_expand_rfs,
  perc_abandon_rfs,
  counties.geom
FROM 
  synthesis.extensification_agroibis_20190130_control 
INNER JOIN

--table to create a fips column that is string instead of numeric
(SELECT 
CONCAT('0',"FIPS"::text) as fips,
"FIPS"
FROM 
synthesis.extensification_agroibis_20190130_control
where length("FIPS"::text) = 4

UNION

SELECT 
"FIPS"::text as fips,
"FIPS"
FROM 
synthesis.extensification_agroibis_20190130_control
where length("FIPS"::text) = 5) as fips_modified

USING("FIPS")

--join to get the geometry column
INNER JOIN spatial.counties 
ON fips_modified.fips = counties.atlas_stco

--used as an interface table(mlra regions and counties)between synthesis.extensification_mlra lrr_groups and the fips values of synthesis.extensification_agroibis_20190130_control 
INNER JOIN synthesis.extensification_county_regions
ON fips_modified.fips=extensification_county_regions.fips

INNER JOIN synthesis.extensification_mlra
ON extensification_county_regions.lrr_group=extensification_mlra.lrr_group