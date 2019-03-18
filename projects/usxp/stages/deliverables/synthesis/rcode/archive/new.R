library(rgdal)     # R wrapper around GDAL/OGR
library(ggplot2)   # for general plotting
library(ggmaps)    # for fortifying shapefiles



# The input file geodatabase
fgdb <- "D:\\projects\\usxp\\deliverables\\maps\\synthesis\\synthesis_intensification.gdb"



# Read the feature class
fc <- readOGR(dsn=fgdb,layer="rfs_intensification_results_counties")

# Next the shapefile has to be converted to a dataframe for use in ggplot2
shapefile_df <- fortify(fc)

# Now the shapefile can be plotted as either a geom_path or a geom_polygon.
# Paths handle clipping better. Polygons can be filled.
# You need the aesthetics long, lat, and group.
map <- ggplot() +
  geom_path(data = shapefile_df,
            aes(x = long, y = lat, group = group),
            color = 'red', fill = 'white', size = .2)

print(map)

# Using the ggplot2 function coord_map will make things look better and it will also let you change
# the projection. But sometimes with large shapefiles it makes everything blow up.
map_projected <- map +
  coord_map()

print(map_projected)



