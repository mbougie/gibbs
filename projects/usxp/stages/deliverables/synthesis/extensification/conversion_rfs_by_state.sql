CREATE TABLE synthesis_extensification.extensification_mlra_states as

SELECT 
-- states.atlas_st, 
 states.atlas_name,
  --mlra_county_regions_rc_expand_mtr3_xmillion_states.count as mtr3_count, 
  --mlra_county_regions_rc_expand_mtr3_xmillion_states.sum as mtr3_sum_mil,
  --mlra_county_regions_rc_expand_mtr3_xmillion_states.sum/1000000 as mtr3_sum,
 --(mlra_county_regions_rc_expand_mtr3_xmillion_states.sum/1000000)/mlra_county_regions_rc_expand_mtr3_xmillion_states.count as mtr3_ratio,
 (mlra_county_regions_rc_expand_mtr3_xmillion_states.sum/1000000)*0.222395 as expansion,
  --mlra_county_regions_rc_abandon_mtr4_xmillion_states.count as mtr4_count, 
  --mlra_county_regions_rc_abandon_mtr4_xmillion_states.sum as mtr4_sum_mil,
 -- mlra_county_regions_rc_abandon_mtr4_xmillion_states.sum/1000000 as mtr4_sum,
 --(mlra_county_regions_rc_abandon_mtr4_xmillion_states.sum/1000000)/mlra_county_regions_rc_abandon_mtr4_xmillion_states.count as mtr4_ratio,
 (mlra_county_regions_rc_abandon_mtr4_xmillion_states.sum/1000000)*0.222395 as abandonment,
 ((mlra_county_regions_rc_expand_mtr3_xmillion_states.sum/1000000)*0.222395) - ((mlra_county_regions_rc_abandon_mtr4_xmillion_states.sum/1000000)*0.222395) as net_conversion
 
FROM 
  synthesis_extensification.mlra_county_regions_rc_abandon_mtr4_xmillion_states
   INNER JOIN
  synthesis_extensification.mlra_county_regions_rc_expand_mtr3_xmillion_states
USING(atlas_st)
  INNER JOIN
  spatial.states
USING(atlas_st)







