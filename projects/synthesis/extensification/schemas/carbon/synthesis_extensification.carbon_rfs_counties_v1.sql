create table synthesis_extensification.carbon_rfs_counties_v1 as 

SELECT 
 
  atlas_stco, 
  'carbon_emissions' as dataset,
  zone_code, 
  count, 
  area, 
  ---gigogram is a 1000 metric tons
  ((44*sum)/12)/1000 as gigagrams_co2e
FROM 
  synthesis_extensification.carbon_emissions_rfs_counties_table

UNION

SELECT 
  atlas_stco, 
  'carbon_sequester' as dataset,
  zone_code, 
  count, 
  area, 
   ---gigagram is a 1000 metric tons
  ((44*sum)/12)/1000 as gigagrams_co2e
FROM 
  synthesis_extensification.carbon_sequester_rfs_counties_table




