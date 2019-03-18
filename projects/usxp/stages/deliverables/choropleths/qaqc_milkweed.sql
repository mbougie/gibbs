CREATE OR REPLACE VIEW main.qaqc_milkweed AS 
 SELECT milkweed.objectid,
    milkweed.atlas_st,
    milkweed.st_abbrev,
    milkweed.state_name,
    milkweed.atlas_stco,
    milkweed.atlas_name,
    milkweed.acres_calc,
    milkweed.expansion_08,
    milkweed.expansion_09,
    milkweed.expansion_10,
    milkweed.expansion_11,
    milkweed.expansion_12,
    milkweed.crploss_08,
    milkweed.crploss_09,
    milkweed.crploss_10,
    milkweed.crploss_11,
    milkweed.crploss_12,
    milkweed.expansion,
    milkweed.crploss,
    milkweed.perc_crploss,
    milkweed.stem_acre,
    counties.geom
   FROM main.milkweed,
    spatial.counties
  WHERE counties.atlas_stco::text = milkweed.atlas_stco;

ALTER TABLE main.qaqc_milkweed
  OWNER TO mbougie;