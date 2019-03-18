--this adds 0 to fromt of fips if its missing
UPDATE synthesis.extensification_county_regions_temp2
SET fips2 = CONCAT('0',fips2::text)
WHERE LENGTH(fips2::text)=4