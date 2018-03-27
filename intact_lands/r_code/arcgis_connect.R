



library(rgdal)     # R wrapper around GDAL/OGR
library(ggplot2)   # for general plotting
library(ggmap)    # for fortifying shapefiles



####################################################################################################

# The input file geodatabase
fgdb <- "D:\\projects\\intact_land\\intact.gdb"

# List all feature classes in a file geodatabase
subset(ogrDrivers(), grepl("GDB", name))

# Read the feature class
fc <- readOGR(dsn=fgdb,layer="disturbed_2015_dissolved")
# Determine the FC extent, projection, and attribute information


map <- ggplot() +
  geom_polygon(data = fc, aes(x = long, y = lat, group = group), fill = 'azure2', size = .6) +
  geom_path(data = shapefile_df2, aes(x = long, y = lat, group = group),color = "lightsteelblue3", size = 1.3) +
  ggtitle("China C02 Emissions") +
  theme(plot.title = element_text(colour = "steelblue",  face = "bold.italic", family = "Helvetica", hjust = 0.5), 
        axis.text.x = element_blank(),
        axis.title.x=element_blank(),
        axis.text.y = element_blank(),
        axis.title.y=element_blank(),
        axis.ticks = element_blank(),
        rect = element_blank()) 

map







