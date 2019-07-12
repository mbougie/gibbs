create table ag_census.ag_census_expansion as 

SELECT 
counties.atlas_name,
counties.atlas_stco,
counties.acres_calc,
agcensus_2007.acres as crop_2007,
agcensus_2017.acres as crop_2017,
agcensus_2017.acres - agcensus_2007.acres as diff,
((agcensus_2017.acres - agcensus_2007.acres)/counties.acres_calc)*100 as perc_conv_county,
counties.geom
FROM
(SELECT 
  sum(value_new) as acres,
  fips
FROM 
  ag_census.agcensus_2007_3metrics
GROUP BY
fips
having sum(value_new)<> 0) as agcensus_2007

inner join

(SELECT 
  sum(value_new) as acres,
  fips
FROM 
  ag_census.agcensus_2017_3metrics
GROUP BY
fips
having sum(value_new)<> 0) as agcensus_2017

USING(fips)

INNER JOIN

spatial.counties ON (agcensus_2007.fips = counties.atlas_stco)

----this selects only the counties that expanded between the years because only concerned with net expansion
WHERE ((agcensus_2017.acres - agcensus_2007.acres)/counties.acres_calc)*100 > 0