----Modifiy extensification.extensification_county_regions 

ALTER TABLE  extensification_mlra.extensification_county_regions
ALTER COLUMN fips TYPE text;

UPDATE
  extensification_mlra.extensification_county_regions set fips= '0'||fips
WHERE char_length(fips) = 4
