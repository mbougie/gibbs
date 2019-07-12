create table synthesis.extensification_mlra_v2 as

SELECT 
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
  ---collect geometries together with similar values (i.e. using the group by clause)
  --sum(ST_Area(mlra_5070.wkb_geometry)) as area,
  sum(shape_area) as area_v2,
  --sum(ST_Area(mlra_5070.wkb_geometry)) * 0.000247105 as acres,
  sum(shape_area) * 0.000247105 as acres_v2,
  --ST_Buffer(ST_Union(ST_Buffer(wkb_geometry, 0.00001)), -0.00001)::Geometry(Polygon,5070) as geom 
  --ST_Buffer(ST_Union(ST_Buffer(mlra_5070.wkb_geometry, 0.00001)), -0.00001) as geom 
FROM 
  synthesis.extensification_price_response, 
  synthesis.extensification_regions, 
  synthesis.mlra_5070
WHERE 
  extensification_regions.lrr_group = extensification_price_response.region
GROUP BY
  extensification_price_response.region, 
  extensification_price_response.expand_from_pasture, 
  extensification_price_response.abandon_to_pasture, 
  extensification_price_response.expand_from_crp, 
  extensification_price_response.abandon_to_crp, 
  extensification_price_response.expand_from_either, 
  extensification_price_response.abandon_to_either



 
