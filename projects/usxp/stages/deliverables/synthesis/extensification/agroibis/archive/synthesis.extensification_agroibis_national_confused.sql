CREATE TABLE synthesis_extensification.extensification_agroibis_national as 
SELECT 
  --fips_modified.fips,
  --extensification_mlra.lrr_group,

  

  --- et ---------------------------------------------------
  --convert acre/ft to gals
  sum((("etExpImp"*ratio_expand_rfs_mtr3) * 325851)/1000000) as gal_et_exp_imp_rfs,
  --convert acre/ft to gals
  sum((("etAbanImp"*ratio_abandon_rfs) * 325851)/1000000) as gal_et_aban_imp_rfs,
  ----------------------------------------------------------

    --convert acre/ft to gals
  sum((("etExpImp"*ratio_expand_rfs_mtr3))) as et_exp_imp_rfs_w_mtr3,
  --convert acre/ft to gals
  sum((("etAbanImp"*ratio_abandon_rfs_mtr4))) as et_aban_imp_rfs_w_mtr4,




  --- irr ---------------------------------------------------
  --convert acre/ft to gals
  sum((("irrExpImp"*ratio_expand_rfs_mlra) * 325851)/1000000) as gal_irr_exp_imp_rfs,

  --convert acre/ft to gals
  sum((("irrAbanImp"*ratio_abandon_rfs_mlra) * 325851)/1000000) as gal_irr_aban_imp_rfs
  ----------------------------------------------------------


FROM 
  synthesis_extensification.extensification_agroibis_20190130_control 
INNER JOIN

--table to create a fips column that is string instead of numeric
(SELECT 
CONCAT('0',"FIPS"::text) as fips,
"FIPS"
FROM 
synthesis_extensification.extensification_agroibis_20190130_control
where length("FIPS"::text) = 4

UNION

SELECT 
"FIPS"::text as fips,
"FIPS"
FROM 
synthesis_extensification.extensification_agroibis_20190130_control
where length("FIPS"::text) = 5) as fips_modified

USING("FIPS")

--join to get the geometry column
INNER JOIN spatial.counties 
ON fips_modified.fips = counties.atlas_stco

--used as an interface table(mlra regions and counties)between synthesis.extensification_mlra lrr_groups and the fips values of synthesis.extensification_agroibis_20190130_control 
INNER JOIN synthesis_extensification.extensification_county_regions
ON fips_modified.fips=extensification_county_regions.fips

INNER JOIN synthesis_extensification.extensification_mlra
ON extensification_county_regions.lrr_group=extensification_mlra.lrr_group