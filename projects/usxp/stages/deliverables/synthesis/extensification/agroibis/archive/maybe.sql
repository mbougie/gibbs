SELECT 
  mlra_5070_dissolved_lrrgroup.lrr_group, 
  extensification_price_response.region, 
  extensification_price_response.expand_from_pasture, 
  extensification_price_response.abandon_to_pasture,
  extensification_price_response.expand_from_pasture - extensification_price_response.abandon_to_pasture as net_pasture,
  extensification_price_response.expand_from_crp, 
  extensification_price_response.abandon_to_crp,
  extensification_price_response.expand_from_crp - extensification_price_response.abandon_to_crp as net_crp, 
  extensification_price_response.expand_from_either, 
  extensification_price_response.abandon_to_either,
  extensification_price_response.expand_from_either - extensification_price_response.abandon_to_either as net_either,
  mlra_5070_dissolved_lrrgroup.acres
FROM 
  synthesis.extensification_price_response, 
  synthesis.mlra_5070_dissolved_lrrgroup
WHERE 
  mlra_5070_dissolved_lrrgroup.lrr_group = extensification_price_response.region;
