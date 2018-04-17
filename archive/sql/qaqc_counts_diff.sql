INSERT INTO qaqc.counts_diff VALUES ('gsConv_new_lcc', (SELECT 
  count_pixel - (SELECT 
  sum(count_pixel) 
FROM 
  qaqc.count
where dataseet = 'gsConv_2013_lcc' or dataseet = 'gsConv_2014_lcc' or dataseet = 'gsConv_2015_lcc') as diff_pixels
FROM 
  qaqc.count
where dataseet = 'gsConv_new' ), (SELECT 
  count_acres - (SELECT 
  sum(count_acres) 
FROM 
  qaqc.count
where dataseet = 'gsConv_2013_lcc' or dataseet = 'gsConv_2014_lcc' or dataseet = 'gsConv_2015_lcc') as diff_pixels
FROM 
  qaqc.count
where dataseet = 'gsConv_new' ), 1 - (SELECT 
  (SELECT 
  sum(count_pixel) 
FROM 
  qaqc.count
where dataseet = 'gsConv_2013_lcc' or dataseet = 'gsConv_2014_lcc' or dataseet = 'gsConv_2015_lcc')/count_pixel
FROM 
  qaqc.count
where dataseet = 'gsConv_new' ))
