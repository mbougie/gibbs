----description: this is a query using rc_nwalt to check the counts of the intial table
----remember nwalt nulls the water and transportation so this will make it so the hectares dont match up when aggregated to the county hectares like the other table
----one check is to group the main table by groups and see how the hectares match up with this table


SELECT
  state_conus.name as state,  
  county_conus.name as county, 
  county_conus.geoid,
  census_2018.tot_pop as census_pop, 
  county_zonal_maj_biomes_60m.majority as biome_maj, 
  county_zonal_hist_nwalt_rc_60m.label as luc,
  nwalt_lookup.initial,
  nwalt_lookup.initial_desc,
  nwalt_lookup.grouped,
  nwalt_lookup.grouped_desc,
  ---county_zonal_hist_nwalt_60m.count,
  county_zonal_hist_nwalt_rc_60m.count * 0.360000010812 as hectares
FROM 
  spatial.county_conus 

  INNER JOIN v3_2.county_zonal_maj_biomes_60m
  ON county_zonal_maj_biomes_60m.geoid = county_conus.geoid

  INNER JOIN amanda.census_2018
  ON census_2018.fips = county_conus.geoid 

  INNER JOIN spatial.state_conus
  ON state_conus.statefp = county_conus.statefp

  --INNER JOIN amanda.county_zonal_hist_nwalt_60m
 -- ON county_conus.geoid = county_zonal_hist_nwalt_60m.atlas_stco 

  INNER JOIN amanda.county_zonal_hist_nwalt_rc_60m
  ON county_conus.geoid = county_zonal_hist_nwalt_rc_60m.atlas_stco 

  INNER JOIN public.nwalt_lookup
  ON nwalt_lookup.grouped = county_zonal_hist_nwalt_rc_60m.label::integer


  