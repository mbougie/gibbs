----Query returned successfully: 26 rows affected, 63 ms execution time.

CREATE TABLE v3_2.county_final_bottom as 
SELECT 
  county_replace.geoid, 
  county_replace.hectares, 
  county_replace.neighbor_list, 
  county_zonal_maj_nwalt_rc_10m.majority as luc,
  county_zonal_maj_biomes_10m.majority as biomes, 
  county_replace.lng, 
  county_replace.lat, 
  county_replace.geom

FROM 
  v3_2.county_replace 
  LEFT OUTER JOIN
  v3_2.county_zonal_maj_nwalt_rc_10m ON county_zonal_maj_nwalt_rc_10m.geoid = county_replace.geoid 
   
  LEFT OUTER JOIN 
  v3_2.county_zonal_maj_biomes_10m ON county_zonal_maj_biomes_10m.geoid = county_replace.geoid




