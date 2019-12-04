/*Description: Dataset contains 

dataset is derived from:
1) extensification_agroibis.countyStats02_20190701
2) extensification_mlra.extensification_county_regions
3) extensification_mlra.extensification_mlra
*/


CREATE TABLE extensification_agroibis.agroibis_counties as 
SELECT 
  "countyStats02_20190701"."FIPS" as fips,
  fips_modified.fips as fips_text,
  extensification_mlra.lrr_group,

  
  ---phosophourous------------------------------------
  ---expansion--- 
  "pExpImp"*extensification_mlra.ratio_expand_rfs_mtr3 as p_exp_imp_rfs,
  ---abandon---
  "pAbanImp"*extensification_mlra.ratio_abandon_rfs_mtr4 as p_aban_imp_rfs,
  ----------------------------------------------------------


  ---nitrogen--------------------------------------
  ---expansion---
  "nExpImp"*extensification_mlra.ratio_expand_rfs_mtr3 as n_exp_imp_rfs, 
  ---abandon---
  "nAbanImp"*extensification_mlra.ratio_abandon_rfs_mtr4 as n_aban_imp_rfs,
  ----------------------------------------------------------


  ------sed--------------------------------------------
 ---expansion---
  "sedExpImp"*extensification_mlra.ratio_expand_rfs_mtr3 as sed_exp_imp_rfs,
  ---abandon---
  "sedAbanImp"*extensification_mlra.ratio_abandon_rfs_mtr4 as sed_aban_imp_rfs,
  ----------------------------------------------------------


  --- et ---------------------------------------------------
  ---expansion---
  ("etExpImp"*extensification_mlra.ratio_expand_rfs_mtr3) as et_exp_imp_rfs,
  --convert acre/ft to gals
  (("etExpImp"*extensification_mlra.ratio_expand_rfs_mtr3) * 325851)/1000000 as et_exp_imp_rfs_gal,

  ---abandon---
  ("etAbanImp"*extensification_mlra.ratio_abandon_rfs_mtr4) as et_aban_imp_rfs,
  --convert acre/ft to gals
  (("etAbanImp"*extensification_mlra.ratio_abandon_rfs_mtr4) * 325851)/1000000 as et_aban_imp_rfs_gal,
  
  ----------------------------------------------------------


  --- irr ---------------------------------------------------
  ---expansion---
  "irrExpImp"*extensification_mlra.ratio_expand_rfs_mtr3 as irr_exp_imp_rfs,
  --convert acre/ft to gals
  (("irrExpImp"*extensification_mlra.ratio_expand_rfs_mtr3) * 325851)/1000000 as irr_exp_imp_rfs_gal,
  
  ---abandon---
  "irrAbanImp"*extensification_mlra.ratio_abandon_rfs_mtr4 as irr_aban_imp_rfs,
  --convert acre/ft to gals
  (("irrAbanImp"*extensification_mlra.ratio_abandon_rfs_mtr4) * 325851)/1000000 as irr_aban_imp_rfs_gal,
  ----------------------------------------------------------

  ---- describe what these are
  extensification_mlra.ratio_expand_rfs_mtr3,
  extensification_mlra.ratio_abandon_rfs_mtr4,
  --counties.atlas_stco,
  counties.geom
FROM 
  extensification_agroibis."countyStats02_20190701"
INNER JOIN


--table to create a fips column that is string instead of numeric
(SELECT 
CONCAT('0',"FIPS"::text) as fips,
"FIPS"
FROM 
extensification_agroibis."countyStats02_20190701"
where length("FIPS"::text) = 4

UNION

SELECT 
"FIPS"::text as fips,
"FIPS"
FROM 
extensification_agroibis."countyStats02_20190701"
where length("FIPS"::text) = 5) as fips_modified

USING("FIPS")


--join to get the geometry column
INNER JOIN spatial.counties 
ON fips_modified.fips = counties.atlas_stco

--used as an interface table(mlra regions and counties)between synthesis.extensification_mlra lrr_groups and the fips values of synthesis.extensification_"countyStats02_20190701"_control 
INNER JOIN extensification_ksu.extensification_county_regions
ON "countyStats02_20190701"."FIPS"=extensification_county_regions.fips

INNER JOIN extensification_ksu.extensification_mlra
ON extensification_county_regions.lrr_group=extensification_mlra.lrr_group