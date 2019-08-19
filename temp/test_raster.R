# library(ggplot2)
# library(maps)
# library(rgdal)# R wrapper around GDAL/OGR
# library(sp)
# library(plyr)
# library(dplyr)
# library(viridis)
# library(scales)
# require(RColorBrewer)
# library(glue)
# library(ggpubr)
# library(cowplot)
# library(RPostgreSQL)
# library(postGIStools)

# data processing
library(ggplot2)
# spatial
library(raster)
library(rasterVis)
library(rgdal)
library(viridis)
require(RColorBrewer)
library(ggmap)




map.tokyo <- get_map("Tokyo")
ggmap(map.tokyo)




########format raster
r <- raster("D:\\projects\\usxp\\series\\s35\\maps\\waterfowl\\tif\\s35_waterfowl_bs3km.tif")
r_spdf <- as(r, "SpatialPixelsDataFrame")
r_df <- as.data.frame(r_spdf)
colnames(r_df) <- c("value", "x", "y")


#### bring in state shapefile for context in map ##################################
state <- readOGR(dsn = "C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\shapefiles.gdb", layer = "states_waterfowl")
state.df <- fortify(state)
# state.df <- state.df[order(state.df$order),]



map <- leaflet(options = leafletOptions(zoomControl = FALSE)) %>%
  addTiles()



map


# ###create panel image ######################
dir = "C:\\Users\\Bougie\\Desktop\\"
fileout=paste(dir,"ggplot2_map_example2",".png", sep="")
ggsave(fileout, width = 20, height = 20, dpi = 800)

# d=data.frame(x1=c(1,3,1,5,4), x2=c(2,4,3,6,6), y1=c(1,1,4,1,3), y2=c(2,2,5,3,5), t=c('a','a','a','b','b'), r=c(1,2,3,4,5))
# ggplot() + 
#   scale_x_continuous(name="x") + 
#   scale_y_continuous(name="y") +
#   geom_rect(data=d, mapping=aes(xmin=x1, xmax=x2, ymin=y1, ymax=y2, fill=t), color="black", alpha=0.5) 



  ggplot() +
    geom_polygon(
         data=state.df,
         aes(x=long, y=lat, group=group),
         fill='#cccccc',
         size=0.25) +

    geom_tile(data=r_df, aes(x=x, y=y, fill=value), alpha=0.8) +

    geom_polygon(
        data=state.df,
        aes(y=lat, x=long, group=group),
        alpha=0,
        colour='white',
        size=0.5
      ) +

    scale_fill_viridis() +
    # coord_map(project="polyconic")+
    # coord_map(projection = "mercator") +
    # coord_cartesian(xlim = c(-115, -90),ylim = c(40, 50)) +
    # coord_map(xlim = c(-115, -90),ylim = c(40, 50)) +

    coord_equal() +
    # coord_map(xlim = c(-115, -90),ylim = c(40, 50)) +

    theme(plot.margin=unit(c(0.5,0,0.5,0.1),"cm"),
          axis.line=element_blank(),
          axis.text.x=element_blank(),
          axis.text.y=element_blank(),
          axis.ticks=element_blank(),
          axis.title.x=element_blank(),
          axis.title.y=element_blank(),
          legend.position = 'bottom',
          legend.justification = 'center',
          legend.key.width = unit(1, 'cm'),
          legend.key.height = unit(0.2, 'cm'),
          legend.title=element_text(size=10),
          legend.text=element_text(size=10),
          legend.margin = unit(0, "cm"),
          panel.background=element_blank(),
          panel.border=element_blank(),
          panel.grid.major=element_blank(),
          panel.grid.minor=element_blank(),
          plot.background=element_blank())+
    guides(fill = guide_colourbar(
      ticks = FALSE,
      frame.colour = "black",
      title.position = "top"
    ))

# ###create panel image ######################
dir = "C:\\Users\\Bougie\\Desktop\\"
fileout=paste(dir,"ggplot2_map_example2",".png", sep="")
ggsave(fileout, width = 20, height = 20, dpi = 800)


##spatial dataframe \\
# mapa <- readOGR(dsn = "D:\\projects\\usxp\\series\\s35\\maps\\breakout\\breakout.gdb",layer="s35_fc_bs3km")
# mapa <- spTransform(mapa, CRS("+init=epsg:5070"))


# root = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\temp\\R_ggplot2_map_demo_2019_06_07\\R_ggplot2_map_demo_2019_06_07\\'
# 
# 
# createMap <- function(obj){
#   
#   mapa <- obj$df
#   
#   ########################################################################
#   ##### modify dataframes #####################################
#   ########################################################################
#   
#   ### create main dataframe (mapa.df)######################################
#   
#   ##spatial dataframe 
#   # mapa <- readOGR(dsn = "C:\\Users\\Bougie\\Box\\data_science_study_group\\resources\\R_ggplot2_map_demo_2019_06_07\\ggplot2_example_features.gdb",layer="agroibis_counties_example")
#   # mapa <- spTransform(mapa, CRS("+init=epsg:5070"))
#   
#   ##This function turns a map into a dataframe that can more easily be plotted with ggplot2.
#   mapa.df <- fortify(mapa)
#   
#   #fortify() creates zany attributes so need to reattach the values from intial dataframe
#   #creates a numeric index for each row in dataframe
#   mapa@data$id <- rownames(mapa@data)
#   
#   #merge the attributes of mapa@data to the fortified dataframe with id column
#   mapa.df <- join(mapa.df, mapa@data, by="id")
#   
#   ##need to do this step sometimes for some dataframes or geometry looks "torn"
#   # mapa.df <- mapa.df[order(mapa.df$order),]
#   
#   ##Use cut() function to divides a numeric vector into different ranges
#   ##note: each bin must: 1)contain a value and 2)no records in dataframe can be null
#   mapa.df$fill = cut(mapa.df$perc, breaks= c(0, 0.5, 2.5, 5, 7.5, 100))
#   
#   
#   
#   
#   
  #### bring in state shapefile for context in map ##################################
  state <- readOGR(dsn = "C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\sf", layer = "states_wgs84")
  state.df <- fortify(state)
#   
#   
#   
#   ########################################################################
#   ##### create graphic ###################################################
#   ########################################################################
#   
#   # There are 3 essential elements to any ggplot call:
#   #   
#   # 1)An aesthetic that tells ggplot which variables are being mapped to the x axis, y axis, (and often other attributes of the graph, such as the color fill). Intuitively, the aesthetic can be thought of as what you are graphing.
#   # 2)A geom or geometry that tells ggplot about the basic structure of the graph. Intuitively, the geom can be thought of as how you are graphing it.
#   # 3)Other options, such as a graph title, axis labels and overall theme for the graph.
#   
#   
#   
  ###ggplot() initializes a ggplot object##############
  ###It can be used to declare the input data frame for a graphic and to specify the set of plot
  ###aesthetics intended to be common throughout all subsequent layers unless specifically overridden.
  d = ggplot() +



    ###A geom tells ggplot about the basic structure of the graph. Intuitively, the geom can be thought of as how you are graphing it.
    ###note: arrangement of geom_polygons important to how they are rendered on map

    ### state grey background ###########
  geom_polygon(
    data=state.df,
    aes(y=lat, x=long, group=group),
    fill='#cccccc'
  ) +

    geom_tile(data=r_df, aes(x=x, y=y, fill=value), alpha=0.8) + 


    ### state boundary strokes ###########
  geom_polygon(
    data=state.df,
    aes(y=lat, x=long, group=group),
    alpha=0,
    colour='white',
    size=0.5
  ) +

    #### define projection of ggplot object #######
  #### did not reproject the actual data just defined the projection of the map
  coord_map(project="polyconic") +

  # coord_cartesian(xlim = c(-115, -90),ylim = c(40, 50)) +
  # coord_map(xlim = c(-115, -90),ylim = c(40, 50)) +
# 
#   coord_equal() +
    
  

    #### add title to map #######
  # labs(title = 'hello') +



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
      text = element_text(color = "#4e4d47", size=30),   ##these are the legend numeric values
      plot.title = element_text(size= 25, vjust=-12.0, hjust=0.20, color = "#4e4d47"),
      plot.caption = element_text(size= 18, color = "blue"),
      legend.position = c(0.12, -0.01)
    ) +

    scale_fill_viridis()
    # scale_colour_gradient2()
    ### create a discrete scale. These functions allow you to specify your own set of mappings from levels in the data to aesthetic values.
    # scale_fill_manual(values = brewer.pal(5, 'YlOrBr')[1:5], ##reference colorbrewer list of hex values
    # 
    #                   ###legend labels
    #                   labels = c("0.5", "2.5", "5", "7.5", ">7.5"),
    # 
    #                   #Legend type guide shows key (i.e., geoms) mapped onto values.
    #                   guide = guide_legend( title='Percent Expansion',
    #                                         title.theme = element_text(
    #                                           size = 32,
    #                                           color = "#4e4d47",
    #                                           vjust=0.0,
    #                                           angle = 0
    #                                         ),
    #                                         # legend bin dimensions
    #                                         keyheight = unit(3, units = "mm"),
    #                                         keywidth = unit(20, units = "mm"),
    # 
    #                                         #legend elements position
    #                                         label.position = "bottom",
    #                                         title.position = 'top',
    # 
    #                                         #The desired number of rows of legends.
    #                                         nrow=1
    # 
    #                   )
    # )
d




