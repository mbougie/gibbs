
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


rm(list = ls())

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

getggplotObject <- function(obj_vector){
  
  ###declare the empty list that will hold all the ggplot objects
  ggplot_object_list <- list()
  
  for (obj in obj_vector){
    
    
    ggplot_object = createMap(obj)
    
    ggplot_object_list <- append(ggplot_object_list, list(ggplot_object))
    
  }
  
  return(ggplot_object_list)
}  
 



############################################################################################
# # panel 1 ################################################################################
###########################################################################################


#### store dataframes in here!?
list_exp <- getggplotObject(list(jsondata$grassland, jsondata$shrubland))
list_aban <- getggplotObject(list(jsondata$forest, jsondata$wetland))
list_aban <- getggplotObject(list(jsondata$wetland))

###create panel image ######################
dir = "I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\x_to_crop\\"
fileout=paste(dir,"test",".png", sep="")

# legend1 <- get_legend(createDummy('acres') + theme(legend.position="bottom", legend.justification="center"))
# legend2 <- get_legend(createDummy('awa') + theme(legend.position="bottom", legend.justification="center"))

col1 = plot_grid(plotlist = list_exp, ncol = 1, nrow = 2, align = 'vh')
col2 = plot_grid(plotlist = list_aban, ncol = 1, nrow = 2, align = 'vh')


# col1 = plot_grid(plotlist = list_exp, ncol = 2, nrow = 1, align = 'vh')
# col2 = plot_grid(plotlist = list_aban, ncol = 1, nrow = 1, align = 'vh')

plot_grid(col1,col2, ncol = 2, rel_heights = c(1, .1))
ggsave(fileout, width = 34, height = 25, dpi = 500)




