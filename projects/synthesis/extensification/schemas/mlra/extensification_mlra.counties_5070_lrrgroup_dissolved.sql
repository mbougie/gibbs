/*
Description: 
This SQL first joins the geometry to the extensification_mlra.extensification_county_regions dataset (see subquery_1) and then
dissolves the counties together based lrr_group column 
*/

----Query returned successfully: 7 rows affected, 8908 ms execution time.

CREATE TABLE extensification_mlra.counties_5070_lrrgroup_dissolved AS
--- this part query dissolves county geom by lrr_group column
 SELECT
 lrr_group,
 ST_Area(ST_Union(ST_SnapToGrid(geom,0.0001))) * 0.00024710538146717 as acres,
 ST_Union(ST_SnapToGrid(geom,0.0001)) as geom

 ---subquery_1: his subquery joins joins the geometry to extensification_mlra.extensification_county_regions
 ---1601 records returned versus 1603 because of commonwelth in Virginia
 FROM (SELECT 
  counties_mod_fips.atlas_stco,
  counties_mod_fips.atlas_name,
  extensification_county_regions.fips,
  counties_mod_fips.fips_numeric,
  extensification_county_regions.lrr_group, 
  extensification_county_regions.mlrarsym, 
  extensification_county_regions.mlra_id, 
  counties_mod_fips.geom
FROM 
  extensification_mlra.extensification_county_regions INNER JOIN

  
---subquery_2: (NOTE:this second subquery is inside the first subquery) This is a subquery in the FROM clause to convert fips text to numeric----------------------------------------------------------
(SELECT
  counties.atlas_stco,
  counties.atlas_name,
  (RIGHT(counties.atlas_st, 1) || counties.cntya)::integer fips_numeric,
  counties.geom
FROM 
  spatial.counties
WHERE LEFT(counties.atlas_st, 1) = '0'

UNION 

SELECT 
  counties.atlas_stco,
  counties.atlas_name,
  (counties.atlas_st || counties.cntya)::integer fips_numeric,
  counties.geom
FROM 
  spatial.counties
WHERE LEFT(counties.atlas_st, 1) != '0'
) as counties_mod_fips
----------------------------------------------------------------------------------------------------------------------

ON
  counties_mod_fips.fips_numeric = extensification_county_regions.fips
)as counties
 GROUP BY lrr_group;



