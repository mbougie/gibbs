
# library(ggplot2)
# library(scales)
# library(maps)
# library(rgdal)# R wrapper around GDAL/OGR
# library(sp)
# require("RPostgreSQL")
# library(plyr)
# library(dplyr)
# library(viridis)
# library(extrafont)
# drv <- dbDriver("PostgreSQL")
# 
# font_import()
# print(fonts())

require(rgdal)

# The input file geodatabase
fgdb <- "D:\\projects\\usxp\\usxp.gdb"

# List all feature classes in a file geodatabase
subset(ogrDrivers(), grepl("GDB", name))
fc_list <- ogrListLayers(fgdb)
print(fc_list)

# Read the feature class
fc_nri <- readOGR(dsn=fgdb,layer="s29_yfc_2009to2012_counties")

df_nri = fc_nri@data
print(df_nri)
# print(df["atlas_name"])