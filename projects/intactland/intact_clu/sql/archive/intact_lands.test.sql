--CREATE TABLE intact_clu.test as 

SELECT 
  intact_clu_ytc.value, 
  intact_clu_ytc.count,
  LEAST(
 NULLIF(intact_clu_ytc.clu_2003_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2004_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2005_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2006_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2007_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2008_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2009_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2011_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2012_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2013_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2014_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2015_crop_d_,0)
  ),
  GREATEST(
 NULLIF(intact_clu_ytc.clu_2003_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2004_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2005_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2006_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2007_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2008_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2009_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2011_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2012_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2013_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2014_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2015_crop_d_,0)
  )
FROM 
  intact_clu.intact_clu_ytc

WHERE LEAST(
 NULLIF(intact_clu_ytc.clu_2003_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2004_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2005_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2006_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2007_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2008_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2009_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2011_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2012_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2013_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2014_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2015_crop_d_,0)
  ) <>
    GREATEST(
 NULLIF(intact_clu_ytc.clu_2003_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2004_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2005_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2006_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2007_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2008_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2009_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2011_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2012_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2013_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2014_crop_d_,0), 
	  NULLIF(intact_clu_ytc.clu_2015_crop_d_,0)
  )
  
ORDER BY count DESC;

--ALTER TABLE intact_clu.test ADD COLUMN ytc text;


--UPDATE intact_clu.test set ytc = 




