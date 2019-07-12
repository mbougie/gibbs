library(rgdal)     # R wrapper around GDAL/OGR
library(ggplot2)   # for general plotting
# library(ggmaps)    # for fortifying shapefiles

# First read in the shapefile, using the path to the shapefile and the shapefile name minus the
# extension as arguments

# oregon <- readOGR(dsn="D:\\projects\\usxp\\current_deliverable\\5_23_18\\sf", layer="tryit_dissolved")
oregon <- readOGR(dsn="D:\\projects\\usxp\\current_deliverable\\5_23_18\\sf", layer="tryit_dissolved")
test_spdf <- as(test, "SpatialPixelsDataFrame")
test_df <- as.data.frame(test_spdf)
colnames(test_df) <- c("value", "x", "y")



# 
# ggplot() +  
#   geom_polygon(data=oregon, aes(x=long, y=lat, group=group),
#                fill=NA, color="grey70", size=0.5) +
#   ggtitle('Expansion of Cropland by Year') +
#   coord_equal() +
#   theme_map() +
#   theme(legend.position="none", plot.title = element_text(hjust = 0.1))


d = (ggplot() +
       geom_polygon(data=test_df, aes(y=y, x=x), fill = 'grey70', colour = 'grey50', size = 0.25) + facet_wrap(~value)+ theme(strip.text.x = element_text(size = 8, colour = "steelblue", face = "bold.italic")) +
       coord_map(project="polyconic") +
       theme(plot.title = element_text(colour = "steelblue",  face = "bold", family = "Helvetica", hjust = 0.5), 
             axis.text.x = element_blank(),
             axis.title.x=element_blank(),
             axis.text.y = element_blank(),
             axis.title.y=element_blank(),
             axis.ticks = element_blank(),
             panel.grid.major = element_blank(),
             legend.position="none"))

d + ggtitle("County CLU Datasets by Year")

