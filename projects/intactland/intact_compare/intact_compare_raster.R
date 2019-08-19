


#load libraries
library(raster)
library(ggplot2)

#open ASCII file using 'raster' command, which converts the ASCII to a raster object
map <- raster('D:\\intactland\\intact_compare\\intact_compare.tif')

library(ggplot2)
library(raster)
library(rasterVis)
library(rgdal)
library(grid)
library(scales)
library(viridis)  # better colors for everyone
library(ggthemes) # theme_map()

datafold <- "D:\\intactland\\intact_compare\\intact_compare_8bit.tif"
ORpath <- "/path/to/Oregon_10N.shp"

test <- raster(datafold) 
# OR <- readOGR(dsn=ORpath, layer="Oregon_10N") 


test_spdf <- as(test, "SpatialPixelsDataFrame")
# test_df <- as.data.frame(test_spdf)
# colnames(test_df) <- c("value", "x", "y")