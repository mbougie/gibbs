create table new.zonal_test_formated

SELECT 
  * 
FROM 
  new.zonal_test
where left(atlas_stco,2) in ('19','27','30','31','38','46','56')