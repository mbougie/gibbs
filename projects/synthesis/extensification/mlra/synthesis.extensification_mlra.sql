CREATE TABLE synthesis_extensification.extensification_mlra as 

SELECT
  counties_5070_lrrgroup_dissolved.objectid as unique_id, 
  counties_5070_lrrgroup_dissolved.lrr_group, 
  extensification_price_response.expand_from_pasture, 
  extensification_price_response.abandon_to_pasture,
  extensification_price_response.expand_from_pasture - extensification_price_response.abandon_to_pasture as net_pasture,
  extensification_price_response.expand_from_crp, 
  extensification_price_response.abandon_to_crp,
  extensification_price_response.expand_from_crp - extensification_price_response.abandon_to_crp as net_crp, 
  extensification_price_response.expand_from_either, 
  extensification_price_response.abandon_to_either,
  extensification_price_response.expand_from_either - extensification_price_response.abandon_to_either as net_either,
  (extensification_price_response.expand_from_either/counties_5070_lrrgroup_dissolved.acres) as ratio_expand_rfs_mlra,
  (extensification_price_response.abandon_to_either/counties_5070_lrrgroup_dissolved.acres) as ratio_abandon_rfs_mlra, 
  --SETH these are next 2 lines are what I am focusing on.  Taking the expand_from_either price response and dividing by the acreage of conversion per the mlra region
  (extensification_price_response.expand_from_either/hist_mtr3.mtr3_acres) as ratio_expand_rfs_mtr3,
  (extensification_price_response.abandon_to_either/hist_mtr4.mtr4_acres) as ratio_abandon_rfs_mtr4,
  counties_5070_lrrgroup_dissolved.acres as mlra_acres,
  hist_mtr3.mtr3_acres,
  hist_mtr4.mtr4_acres,
  counties_5070_lrrgroup_dissolved.geom
  
  
FROM 
  synthesis_extensification.extensification_price_response, 
  synthesis_extensification.counties_5070_lrrgroup_dissolved,


---make temp tables from histgrams to get the values of mtr per lrr_group
--- total acres of USXP expansion per MLRA region
(SELECT
label as mtr3,
variable as lrr_group,
acres as mtr3_acres
FROM
synthesis_extensification.extensification_mlra_mtr_counts
WHERE label='3') as hist_mtr3,


----total acres of USXP abandonment per MLRA region
(SELECT
label as mtr4,
variable as lrr_group,
acres as mtr4_acres
FROM
synthesis_extensification.extensification_mlra_mtr_counts
WHERE label='4') as hist_mtr4




--join every table by lrr_group
WHERE 
  counties_5070_lrrgroup_dissolved.lrr_group = extensification_price_response.region AND counties_5070_lrrgroup_dissolved.lrr_group = hist_mtr3.lrr_group AND counties_5070_lrrgroup_dissolved.lrr_group = hist_mtr4.lrr_group
