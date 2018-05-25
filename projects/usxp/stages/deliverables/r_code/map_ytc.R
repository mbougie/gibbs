library(raster)
library(rgdal)

# Load raster into R
df <- raster("C:\\Users\\Bougie\\Desktop\\temp\\agrigate_100.tif")

# View raster structure
 



## class       : RasterLayer 
## dimensions  : 1367, 1697, 2319799  (nrow, ncol, ncell)
## resolution  : 1, 1  (x, y)
## extent      : 731453, 733150, 4712471, 4713838  (xmin, xmax, ymin, ymax)
## coord. ref. : +proj=utm +zone=18 +datum=WGS84 +units=m +no_defs +ellps=WGS84 +towgs84=0,0,0 
## data source : /Users/mjones01/Documents/data/NEON-DS-Airborne-Remote-Sensing/HARV/DSM/HARV_dsmCrop.tif 
## names       : HARV_dsmCrop 
## values      : 305.07, 416.07  (min, max)

# plot raster
# note \n in the title forces a line break in the title

# s <- raster(nrow=10, ncol=10)
# r <- resample(df, s, method='bilinear')
# 
# 
# plot(df, main="NEON Digital Surface Model\nHarvard Forest")

library(ggplot2)
library(raster)
library(rasterVis)
library(rgdal)
library(grid)
library(scales)
library(viridis)  # better colors for everyone
library(ggthemes) # theme_map()
require("RPostgreSQL")
# library(plyr)
# library(dplyr)
# library(viridis)
# library(extrafont)
drv <- dbDriver("PostgreSQL")

# datafold <- "/path/to/oregon_masked_tmean_2013_12.tif"
# ORpath <- "/path/to/Oregon_10N.shp"
con <- dbConnect(drv, dbname = "usxp",
                 host = "144.92.235.105", port = 5432,
                 user = "mbougie", password = "Mend0ta!")

# df_1 <- dbGetQuery(con, "SELECT name, group_name, count, acres, round(perc::numeric,1) as perc, color FROM counts_cdl.s9_ytc_fc_fnl6 WHERE perc > 1.5  and name != 'Grassland/Pasture' or (name = 'Durum Wheat' and color IS NOT NULL) ")
df <- dbGetQuery(con, "SELECT value::text as value, color_ytc FROM misc.lookup_yxc")




test <- raster("D:\\projects\\usxp\\current_deliverable\\5_23_18\\s22_ytc_bs100_rs.tif")
oregon <- readOGR(dsn="C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\sf", layer="states") 

test_spdf <- as(test, "SpatialPixelsDataFrame")
test_df <- as.data.frame(test_spdf)
colnames(test_df) <- c("value", "x", "y")

jColors <- df$color
print(jColors)
names(jColors) <- df$value
print(head(jColors))

####note the as.factor() is very important for continoeus values using disctere color scales!!!!
ggplot() +  
  geom_tile(data=test_df, aes(x=x, y=y, fill=as.factor(value)), alpha=0.8) + 
  geom_polygon(data=oregon, aes(x=long, y=lat, group=group),
               fill=NA, color="grey70", size=0.5) +
  scale_fill_manual(values = jColors)+
  ggtitle('Expansion of Cropland by Year') +
  coord_equal() +
  theme_map() +
  theme(legend.position="none", plot.title = element_text(hjust = 0.1))


