

CREATE TABLE qaqc.county_regions_lrr_group_dissolve AS

 SELECT 
 lrr_group,
 ST_Union(ST_SnapToGrid(geom,0.0001)) as geom
 FROM (SELECT 
  yo.atlas_stco,
  yo.atlas_name,
  extensification_county_regions.fips, 
  extensification_county_regions.lrr_group, 
  extensification_county_regions.mlrarsym, 
  extensification_county_regions.mlra_id, 
  yo.geom
FROM 
  extensification_mlra.extensification_county_regions INNER JOIN
(SELECT 
  counties.atlas_stco,
  counties.atlas_name,
  (RIGHT(counties.atlas_st, 1) || counties.cntya)::integer fips_num,
  counties.geom
FROM 
  spatial.counties
WHERE LEFT(counties.atlas_st, 1) = '0'

UNION 

SELECT 
  counties.atlas_stco,
  counties.atlas_name,
  (counties.atlas_st || counties.cntya)::integer fips_num,
  counties.geom
FROM 
  spatial.counties
WHERE LEFT(counties.atlas_st, 1) != '0'
) as yo

ON
  yo.fips_num = extensification_county_regions.fips
)as parishes
 GROUP BY lrr_group;



