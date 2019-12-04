rm(list = ls())

library(ggplot2)
library(maps)
library(rgdal)# R wrapper around GDAL/OGR
library(sp)
require(RPostgreSQL)
library(postGIStools)
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
require(rgdal)
require(raster)
require(rpostgis)
# require(devtools)
# 
user <- "mbougie"
host <- '144.92.235.105'
port <- '5432'
password <- 'Mend0ta!'

### Make the connection to database ######################################################################
con <- dbConnect(PostgreSQL(), dbname = 'synthesis', user = user, host = host, port=port, password = password)


fc <- get_postgis_query(con, "SELECT * FROM intensification_ksu.rfs_intensification",
                  geom_name = "geom_4326")

proj = proj4string(fc)
print(proj)



ROOT = 'H:\\new_data_8_18_19\\d_drive\\synthesis\\s35\\intensification\\schemas\\agroibis\\Int04\\GeoTIFF\\GeoTIFF'
setwd(ROOT)

img <- list.files(pattern='.tif$')
s <- raster::stack(img)



fc_temp = extract(s, fc, method = 'simple', sp = T, na.rm = F)

fc_temp.df = fc_temp@data

# header.df = fc_temp.df[0,]

# dbWriteTable(con, c('intensification_ksu','rfs_intensification_rasters'), header.df, overwrite = FALSE)
# dbWriteTable(con, 'header_test', fc_temp.df, overwrite = FALSE)


# postgis_insert(con, fc_temp, 'header_test', write_cols = NA,
#                geom_name = 'geom_4152', hstore_name = NA_character_)

# 
# dirs = list.dirs()
# dirs = dirs[grepl('/', dirs, fixed = T)]
# 
# # for each directory (i.e. rotation)
# for(dir in dirs){
#   
#   #get fesh version of fc
#   fc_temp = fc
#   
#   # link to directory and list .tif files
#   setwd(dir); print(getwd())
#   layers = list.files(pattern = '.tif')
#   
#   # since raster extents are different and stack can't be used...
#   # for each raster in directory
#   for(layer in layers){
#     
#     # load raster
#     r = raster(layer)
#     r = projectRaster(r, crs = proj)
#     
#     # for N2O and Napp rasters, set zero values to NA
#     if(grepl('N2O', layer, fixed = T) | grepl('Napp', layer, fixed = T)){
#       r[r == 0] = NA
#     }
#     
#     # extract data to fc
#     fc_temp = extract(r, fc_temp, method = 'simple', sp = T, na.rm = F)
#   }
#   print(ncol(fc_temp@data))
#   write.csv(fc_temp@data, 'intens_patch_biophysics.csv')
#   setwd(ROOT); print(getwd())
# }

# setwd("F:/RFS_Intensification/biophysical rasters/cc")
# df = read.csv('intens_patch_biophysics.csv')
# nrow(df)
# df_comp = df[complete.cases(df),]
# nrow(df_comp)
# 2560567/3602949
