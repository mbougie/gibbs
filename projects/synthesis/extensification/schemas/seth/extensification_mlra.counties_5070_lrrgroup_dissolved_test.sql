/*
Description: 
This SQL first joins the geometry to the extensification_mlra.extensification_county_regions dataset (see subquery_1) and then
dissolves the counties together based lrr_group column 
*/

----Query returned successfully: 7 rows affected, 8908 ms execution time.
/*
CREATE TABLE public.counties_5070_lrrgroup_dissolved_t3 AS
--- this part query dissolves county geom by lrr_group column
 SELECT
 lrr_group,
 ST_Area(ST_Union(ST_SnapToGrid(geom,0.0001))) * 0.00024710538146717 as acres,
 ST_Union(ST_SnapToGrid(geom,0.0001)) as geom

 ---subquery_1: this subquery joins joins the geometry to extensification_mlra.extensification_county_regions
 ---1601 records returned versus 1603 because of commonwelth in Virginia
 FROM 
 (SELECT 
  counties.atlas_stco,
  counties.atlas_name,
  counties.fips,
  a.lrr_group, 
  counties.geom
FROM 
  extensification_seth.abd_acres_fips_summary as a INNER JOIN
spatial.counties  
ON
  counties.fips = a.atlas_stco
)as counties


 GROUP BY lrr_group;
*/



---CREATE TABLE public.counties_5070_lrrgroup_dissolved AS
--- this part query dissolves county geom by lrr_group column
 SELECT
  lrr_group,
sum(mean),
ST_Area(ST_Union(ST_SnapToGrid(geom,0.0001))) * 0.00024710538146717 as acres,
(sum(mean)/(ST_Area(ST_Union(ST_SnapToGrid(geom,0.0001))) * 0.00024710538146717))*100 as current_field,
st_transform(ST_Union(ST_SnapToGrid(geom,0.0001)),4326) as geom
FROM 
extensification_seth.ext_acres_fips_summary as a INNER JOIN
spatial.counties  
ON
counties.fips = a.atlas_stco
INNER JOIN misc.conversion_table ON 'acre' = conversion_table.extensification GROUP BY lrr_group;
