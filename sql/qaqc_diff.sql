SELECT 
  count_pixel - (SELECT 
  sum(count_pixel) 
FROM 
  qaqc.count
where dataseet = 'gsConv_2013_lcc' or dataseet = 'gsConv_2014_lcc' or dataseet = 'gsConv_2015_lcc') as diff_pixels
FROM 
  qaqc.count
where dataseet = 'gsConv_new' 




