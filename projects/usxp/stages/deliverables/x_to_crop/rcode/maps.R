library(ggplot2)
library(maps)
library(rgdal)# R wrapper around GDAL/OGR
library(sp)
library(plyr)
# library(dplyr)
library(viridis)
library(scales)
require(RColorBrewer)
library(glue)
# library(ggpubr)
library(cowplot)
library(RPostgreSQL)
library(postGIStools)



# root = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\temp\\R_ggplot2_map_demo_2019_06_07\\R_ggplot2_map_demo_2019_06_07\\'



createMap <- function(obj){
  
  #### bring in state shapefile for context in map ##################################
  state.df <- readOGR(dsn = "H:\\new_data_8_18_19\\e_drive\\data\\general\\sf", layer = "states")

  
  map <- obj$df
  ###############################################
  #### graphics #################################
  ###############################################
  
  
 d <- ggplot() + 
    
  # state boundary background ###########
  ### state grey background ###########
  geom_polygon(
    data=state.df,
    aes(y=lat, x=long, group=group),
    fill='#7e7e7e'
  ) +
    
    geom_tile(
      data=map,
      aes(x=x,y=y, fill=map$value)) +
    
    
    ### state boundary strokes ###########
  geom_polygon(
    data=state.df,
    aes(y=lat, x=long, group=group),
    alpha=0,
    colour='white',
    size=0.5
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
    
    
    # scale_fill_gradientn(colours=c("#fcff84","#50ca66","#1b2380")) + 
    scale_fill_gradientn(colours=obj$palette) + 
    
    
    
    
    
    guides(fill = guide_colorbar(title='', reverse = FALSE, barwidth = unit(125, units = "mm"), barheight =  unit(5, units = "mm"), title.position = 'top')) + theme(legend.direction = "horizontal") 
 
 return(d) 
}

# 
# 
# 
# 
