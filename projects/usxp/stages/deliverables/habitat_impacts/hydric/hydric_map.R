rm(list = ls())

require(ggplot2)
require(rgdal)
require(sp)
require(raster)
require(RColorBrewer)
require(ggpubr)
require("RPostgreSQL")
library(postGIStools)
require(sqldf)



library(rasterVis)

library(grid)
library(scales)
library(viridis)  # better colors for everyone
library(ggthemes) # theme_map()

user <- "mbougie"
host <- '144.92.235.105'
port <- '5432'
password <- 'Mend0ta!'

### Make the connection to database ######################################################################
con_synthesis <- dbConnect(PostgreSQL(), dbname = 'usxp_deliverables', user = user, host = host, port=port, password = password)










### Expansion:attach df to specific object in json #####################################################
states = get_postgis_query(con_synthesis, "SELECT * FROM spatial.states",
                                                geom_name = "geom")


setwd('I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\hydric\\tif2')
r1 = raster('hydric_bs3km.tif')

### convert the raster to SPDF
r1_spdf <- as(r1, "SpatialPixelsDataFrame")

###make the SPDF to a regular dataframe
r1_df <- as.data.frame(r1_spdf)

####add columns to the dataframe
colnames(r1_df) <- c("value", "x", "y")

hist(r1_df$value,breaks = 50)

# r1_df$value[r1_df$value>20] <- 20
# 
# hist(r1_df$value,breaks = 50)








###############################################
#### graphics #################################
###############################################


ggplot() + 

## state boundary background ###########
geom_polygon(
  data=states,
  aes(x=long,y=lat,group=group),
  fill='#808080') +
  
geom_tile(
 data=r1_df,
 aes(x=x,y=y, fill=r1_df$value)) +


### state boundary strokes ###########
geom_polygon(
  data=states,
  aes(y=lat, x=long, group=group),
  alpha=0,
  colour='white',
  size=3
) +
  
 
# coord_map(project="polyconic") + 
  coord_equal() + 

  
  #### add title to map #######
#### add title to map #######
labs(title = '') + 
  
  
  
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
    legend.position = c(0.08, 0.09),   ####(horizontal, vertical)
    text = element_text(color = "#4e4d47", size=30)  ##these are the legend numeric values
  ) +
  
  
  ###this is modifying the specifics of INSIDE the legend (i.e. the legends components that make up the legend)
  ### create a discrete scale. These functions allow you to specify your own set of mappings from levels in the data to aesthetic values.
  # scale_fill_gradient2(low = "#fcff84",
  #                     mid="#50ca66",
  #                     high = '#1b2380'
  # ) + 
  
  
  scale_fill_gradientn(colours=c("#fcff84","#50ca66","#1b2380")) + 
  
  
  
  
  
  guides(fill = guide_colorbar(title='', reverse = FALSE, barwidth = unit(125, units = "mm"), barheight =  unit(5, units = "mm"), title.position = 'top')) + theme(legend.direction = "horizontal") 


fileout = 'I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\hydric\\deliverables\\test.png'
ggsave(fileout, width = 34, height = 25, dpi = 500)