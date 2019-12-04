# library(ggplot2)
# library(maps)
# library(rgdal)# R wrapper around GDAL/OGR
# library(sp)
# library(plyr)
# # library(dplyr)
# library(viridis)
# library(scales)
# require(RColorBrewer)
# library(glue)
# # library(ggpubr)
# library(cowplot)
# library(RPostgreSQL)
# library(postGIStools)
# #
# #
# #
# library(rasterVis)
# 
# library(grid)
# library(scales)
# library(viridis)  # better colors for everyone
# library(ggthemes) # theme_map()
# 
# user <- "mbougie"
# host <- '144.92.235.105'
# port <- '5432'
# password <- 'Mend0ta!'
# 
# ### Make the connection to database ######################################################################
# con_synthesis <- dbConnect(PostgreSQL(), dbname = 'usxp_deliverables', user = user, host = host, port=port, password = password)


legendThreshold <- function(threshold){
  ###description: takes input desired threshold of legend and return the raw number needed to acheive the input value
  
  ### conversion between 9km qsuared and acres
  acres_in_9km = 2223.95
  
  ###reduce ratio to stems per acre
  stems_per_acres = threshold/10000
  
  #### number of stems needed in a 9 km squared block to achieve the deired input threshold
  stems_per_9km = stems_per_acres * acres_in_9km
  print(stems_per_9km)
  return(stems_per_9km)
}

legendThreshold(5000)

setwd('I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\habitat_impacts\\milkweed\\data\\tiffs')
r = raster('milkweed_bs3km_region.tif')

### convert the raster to SPDF
r_spdf <- as(r, "SpatialPixelsDataFrame")

###make the SPDF to a regular dataframe
r_df <- as.data.frame(r_spdf)

####add columns to the dataframe
colnames(r_df) <- c("value", "x", "y")

#### stats
## get the counts per bin
table(r_df$value)
hist(r_df$value, breaks = 500)


##### perform stretching procedures to the dataset ##################
#### if number of stems per block is less than 5 convert to NA
r_df$value[r_df$value<legendThreshold(500)] <- NA

#### Set the saturation limit 255 at 50,000 stems per block 
r_df$value[r_df$value>legendThreshold(100000)] <- 100000
# r_df$value = log(r_df$value)
hist(r_df$value, breaks = 1000)
table(r_df$value)

#####################################################################

### Expansion:attach df to specific object in json #####################################################
states_region = get_postgis_query(con_synthesis, "SELECT geom FROM spatial.states WHERE st_abbrev
                                           IN ('IL','IN','IA','KS','KY','MI','MN','MO','NE','ND','OH','SD','WI')",
                                                geom_name = "geom")

states_region.df <- fortify(states_region)

### Expansion:attach df to specific object in json #####################################################
states_large = get_postgis_query(con_synthesis, "SELECT geom FROM spatial.states",
                           geom_name = "geom")

states_large.df <- fortify(states_large)





###############################################
#### graphics #################################
###############################################


d <- ggplot() + 

### states_large background ###########
geom_polygon(
  data=states_large,
  aes(x=long,y=lat,group=group),
  # fill='#D0D0D0') +
  fill='#f0f0f0') +
  
### states_large boundary strokes ###########
geom_polygon(
  data=states_large,
  aes(y=lat, x=long, group=group),
  alpha=0,
  colour='white',
  size=6
) +
### states_region boundary background ###########
  geom_polygon(
  data=states_region,
  aes(x=long,y=lat,group=group),
  # fill='#D0D0D0') +
  fill='#808080') +

###focus datset
  geom_tile(
    data=r_df,
    #### using alpha greatly increases the render time!!!!  --avoid if when possible
    # alpha=0.8,
    aes(x=x, y=y,fill=value)
  ) +

## states_region boundary strokes ###########
geom_polygon(
  data=states_region,
  aes(y=lat, x=long, group=group),
  alpha=0,
  colour='white',
  size=2
) +


######have to use projection limits and NOT lat long
coord_equal(xlim = c(-660000,1250000), ylim = c(1300000,2850000)) + 

#### add title to map #######
labs(title = '')  +



  theme(
    #### nulled attributes ##################
    axis.text.x = element_blank(),
    axis.title.x=element_blank(),
    axis.text.y = element_blank(),
    axis.title.y=element_blank(),
    axis.ticks = element_blank(),
    axis.line = element_blank(),

    panel.background = element_rect(fill = NA, color = NA),
    panel.grid.major = element_blank(),

    plot.background = element_rect(fill = NA, color = NA),
    plot.margin = unit(c(0, 0, 0, 0), "cm"),

    #### modified attributes ########################
    ##parameters for the map title
    plot.title = element_text(size= 45, vjust=-12.0, hjust=0.10, color = "#4e4d47"),
    ##shifts the entire legend (graphic AND labels)
    legend.justification = c(0,0),
    legend.position = c(0.31, 0.04),   ####(horizontal, vertical)

    text = element_blank()  ##these are the legend numeric values
  ) +


  ###this is modifying the specifics of INSIDE the legend (i.e. the legends components that make up the legend)
  ### create a discrete scale. These functions allow you to specify your own set of mappings from levels in the data to aesthetic values.
  scale_fill_gradient(low = "#ffe3bf",
                      high = '#f38423'
                      ) +




guides(fill = guide_colorbar(reverse = FALSE, barwidth = unit(0.30, units = "npc"), barheight = unit(0.015, units = "npc"), title.position = 'top')) + theme(legend.direction = "horizontal")



getggplotObject <- function(cnt, multiplier, slots, labels){

  ###declare the empty list that will hold all the ggplot objects
  ggplot_object_list <- list()

  # cnt = 0.015
  print('slots')
  print(slots)
  limit = cnt + (multiplier * 2)
  print('limit')
  print(limit)

  i = 1
  # labels <- c("20%","40%","60%","80%",">80%")
  while (cnt <= limit) {
    print(cnt)
    ggplot_object = annotation_custom(grobTree(textGrob(labels[i], x=cnt, y= 0.02, rot = 0,gp=gpar(col="#4e4d47", fontsize=45, fontface="bold"))))
    ggplot_object_list <- append(ggplot_object_list, list(ggplot_object))
    cnt = cnt + multiplier
    i = i + 1
  }
  return(ggplot_object_list)
}


legend_title = annotation_custom(grobTree(textGrob("Milkweeds Lost (stems / 10,000 acres)", x=0.51, y= 0.09, rot = 0,gp=gpar(col="#4e4d47", fontsize=45, fontface="bold"))))
#####NOTE: the values are scaled to 10,000 acres 
legendlabels_abandon <- getggplotObject(cnt = 0.315, multiplier = 0.17, slots = 3, labels = c("500","50,000",">100,000"))



d + legend_title + legendlabels_abandon

fileout = 'I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\habitat_impacts\\milkweed\\deliverables\\test.png'
ggsave(fileout, width = 34, height = 25, dpi = 500)

