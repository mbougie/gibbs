create table synthesis_extensification.carbon_rfs_counties_v2 as 

SELECT
counties.atlas_stco,
carbon_emissions.count as e_count, 
carbon_emissions.area as e_area,
carbon_emissions.mg_c as e_mg_c,
carbon_emissions.gigagrams_co2e as e_gigagrams_co2e,
carbon_sequester.count as s_count, 
carbon_sequester.area as s_area,
carbon_sequester.mg_c as s_mg_c,
carbon_sequester.gigagrams_co2e as s_gigagrams_co2e,
counties.geom

FROM
(SELECT 
  atlas_stco, 
  'carbon_emissions' as dataset,
  zone_code, 
  count, 
  area, 
  sum as mg_c,
  ---gigogram is a 1000 metric tons
  ((44*sum)/12)/1000 as gigagrams_co2e
FROM 
  synthesis_extensification.carbon_emissions_rfs_counties_table) as carbon_emissions

FULL OUTER JOIN 

(SELECT 
  atlas_stco, 
  'carbon_sequester' as dataset,
  zone_code, 
  count, 
  area,
  sum as mg_c, 
   ---gigagram is a 1000 metric tons
  ((44*sum)/12)/1000 as gigagrams_co2e
FROM 
  synthesis_extensification.carbon_sequester_rfs_counties_table) as carbon_sequester

USING(atlas_stco)

JOIN 

spatial.counties

USING(atlas_stco)


