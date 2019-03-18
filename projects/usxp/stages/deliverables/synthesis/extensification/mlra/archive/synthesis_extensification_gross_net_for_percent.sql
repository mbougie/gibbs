SELECT 
  (sum(extensification_mlra.expand_from_either)/(SELECT sum(mtr3_acres)FROM synthesis_extensification.extensification_mlra))*100
FROM 
  synthesis_extensification.extensification_mlra
--where extensification_mlra.expand_from_either < 0




SELECT 
  (sum(extensification_mlra.abandon_to_either)/(SELECT sum(mtr4_acres)FROM synthesis_extensification.extensification_mlra))*100
FROM 
  synthesis_extensification.extensification_mlra
--where extensification_mlra.abandon_to_either > 0
