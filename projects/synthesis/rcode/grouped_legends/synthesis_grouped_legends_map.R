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



createMap <- function(mapa, obj, panel_params, figure_params){
  
  
  palette=figure_params$palette
  bin_breaks= obj$bin_breaks
  title =panel_params$title
  
  #### get datasets######################
  
  state.df <- readOGR(dsn = "H:\\new_data_8_18_19\\e_drive\\data\\general\\sf", layer = "states")
  
  
  ########################################################################
  ##### modify dataframes ################################################
  ########################################################################
  
  #fortify() creates zany attributes so need to reattach the values from intial dataframe
  mapa.df <- fortify(mapa)
  
  #creates a numeric index for each row in dataframe
  mapa@data$id <- rownames(mapa@data)
  
  #merge the attributes of mapa@data to the fortified dataframe with id column
  mapa.df <- join(mapa.df, mapa@data, by="id")
  
  
  ####create new fill dataset with cut values returned from getFillValues() function
  # mapa.df$fill = getFillValues()
  mapa.df$fill = cut(mapa.df$current_field, breaks= bin_breaks)
  
  
  
  ########################################################################
  ##### private functions ################################################
  ########################################################################
  
  #### function to get legend range vector for color pallette that is derived from breaks vector in json
  getLegendRange <- function(breaks){
    v <- 1:10
    print(v)
    print(breaks)
    print('-------------------------')
    
    
    
    print(sum(breaks < 0))
    ### for negative values
    total = 5 - (sum(breaks < 0))
    print(total)
    if(total != 0){v <- head(v, -total)}
    print(total)
    print(v)
    
    
    
    
    #### for postive values
    total = 5 - (sum(breaks > 0))
    if(total != 0){v <- v[!v %in% 1:total]}
    
    print('-------------------------')
    print(v)
  }
  
  getLegendRange(bin_breaks)
  
  
  
  
  
  ########################################################################
  ##### create graphic ###################################################
  ########################################################################
  
  d = ggplot() +
    
    
    ### state grey background ###########
  geom_polygon(
    data=state.df,
    aes(y=lat, x=long, group=group),
    fill='#cccccc'
  ) +
    
    
    ### county choropleth map ###########
  geom_polygon(
    data=mapa.df,
    aes(y=lat, x=long, group=group, fill = fill),
    colour = '#cccccc',
    size = 0.5
  ) +
    
    
    
    ### state boundary strokes ###########
  geom_polygon(
    data=state.df,
    aes(y=lat, x=long, group=group),
    alpha=0,
    colour='white',
    size=0.5
  ) +
    
    
    # Equal scale cartesian coordinates
    ####NOTE: besure to set clip to off or the grob annotation is clipped by the dimensions of the panel
    coord_equal(clip = 'off') +
    
    #### add title to map #######
  labs(title = title) +
    
    
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
      
      
      #### modify title ########################
      plot.title = element_text(size= 40, vjust=-0.20, hjust=0.0, color = "#4e4d47"),
      
      #### modify title ########################
      legend.position="none"
      
    ) +
    
    
    ### create a discrete scale. These functions allow you to specify your own set of mappings from levels in the data to aesthetic values.
    scale_fill_manual(values = rev(brewer.pal(10, palette)[getLegendRange(obj$bin_breaks)]))
  
  
  return(d)
  
}





