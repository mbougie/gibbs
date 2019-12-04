
library(ggplot2)
library(maps)
library(rgdal)# R wrapper around GDAL/OGR
library(sp)
require("RPostgreSQL")
library(plyr)
# library(dplyr)
library(viridis)
library(scales)
library(rjson)
# library(jsonlite)
require(RColorBrewer)
library(glue)
# library(ggpubr)
library(cowplot)
library(lattice)
library(gridExtra)
library(grid)


rm(list = ls())





# library(gridExtra)
# library(grid)
# library(ggplot2)
# library(lattice)
# p <- qplot(1,1)
# p2 <- xyplot(1~1)
# r <- rectGrob(gp=gpar(fill="grey90"))
# t <- textGrob("text")
# grid.arrange(t, p, p2, r, ncol=2)

###########################################################################################
##### environment #########################################################################
###########################################################################################
rootpath = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\usxp\\stages\\deliverables\\x_to_crop\\'

#####link to the other two scripts
source(paste(rootpath, 'rcode\\maps.R', sep='\\'))

json_file = paste(rootpath, 'json\\json_panels.json', sep='\\')
jsondata <- fromJSON(file=json_file)


setwd('I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\x_to_crop\\tif')



###########################################################################################
##### dataframe ###########################################################################
###########################################################################################
user <- "mbougie"
host <- '144.92.235.105'
port <- '5432'
password <- 'Mend0ta!'


#### grassland ################################################
r_grassland = raster('s35_agg_3km_sum_grassland.tif')

### convert the raster to SPDF
r_grassland.spdf <- as(r_grassland, "SpatialPixelsDataFrame")

###make the SPDF to a regular dataframe
r_grassland.df <- as.data.frame(r_grassland.spdf)

####add columns to the dataframe
colnames(r_grassland.df) <- c("value", "x", "y")

hist(r_grassland.df$value,breaks = 50)

###attach df to specific object in json
jsondata$grassland$df <- r_grassland.df




#### forest ################################################
r_forest = raster('s35_agg_3km_sum_forest.tif')

### convert the raster to SPDF
r_forest.spdf <- as(r_forest, "SpatialPixelsDataFrame")

###make the SPDF to a regular dataframe
r_forest.df <- as.data.frame(r_forest.spdf)

####add columns to the dataframe
colnames(r_forest.df) <- c("value", "x", "y")

hist(r_forest.df$value,breaks = 50)

###attach df to specific object in json
jsondata$forest$df <- r_forest.df





#### shrubland ################################################
r_shrubland = raster('s35_agg_3km_sum_shrubland.tif')

### convert the raster to SPDF
r_shrubland.spdf <- as(r_shrubland, "SpatialPixelsDataFrame")

###make the SPDF to a regular dataframe
r_shrubland.df <- as.data.frame(r_shrubland.spdf)

####add columns to the dataframe
colnames(r_shrubland.df) <- c("value", "x", "y")

hist(r_shrubland.df$value,breaks = 50)

###attach df to specific object in json
jsondata$shrubland$df <- r_shrubland.df


#### wetland ################################################
r_wetland = raster('s35_agg_3km_sum_wetland.tif')

### convert the raster to SPDF
r_wetland.spdf <- as(r_wetland, "SpatialPixelsDataFrame")

###make the SPDF to a regular dataframe
r_wetland.df <- as.data.frame(r_wetland.spdf)

####add columns to the dataframe
colnames(r_wetland.df) <- c("value", "x", "y")

hist(r_wetland.df$value,breaks = 50)

###attach df to specific object in json
jsondata$wetland$df <- r_wetland.df







######################################################################################################
############  get ggplot objects #####################################################################
######################################################################################################

# getggplotObject <- function(obj_vector){
#   
#   ###declare the empty list that will hold all the ggplot objects
#   ggplot_object_list <- list()
#   
#   for (obj in obj_vector){
#     
#     
#     ggplot_object = createMap(obj)
#     
#     ggplot_object_list <- append(ggplot_object_list, list(ggplot_object))
#     
#   }
#   
#   return(ggplot_object_list)
# }  
# 


getggplotObject <- function(obj_vector){
  

  ggplot_object = createMap(obj_vector)
    

  
  return(ggplot_object)
}  
 



############################################################################################
# # panel 1 ################################################################################
###########################################################################################


#### store dataframes in here!?
p1 <- getggplotObject(jsondata$grassland)
p2 <- getggplotObject(jsondata$grassland)
p3 <- getggplotObject(jsondata$wetland)

###create panel image ######################
dir = "I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\x_to_crop\\"
fileout=paste(dir,"test",".png", sep="")

# ###create a matrix that will be filled with the plots above
lay <- rbind(c(NA,1,1,1,1),
             c(NA,1,1,1,1),
             c(2,2,2,1,1))
# 
# #merge all three plots within one grid (and visualize this)
g <- arrangeGrob(p1,p2,p3, layout_matrix = lay)

#arranges plots within grid
# g <- arrangeGrob(p1,p2,p3, nrow=2)

ggsave(fileout, width = 34, height = 25, dpi = 500, g)



