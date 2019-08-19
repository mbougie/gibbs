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



createMap <- function(obj){
  print('obj$column')
  print(obj$column)
  ### get the spatial dtaframe that is stored in the object
  mapa <- obj$df
  
  ########################################################################
  ##### modify dataframes #####################################
  ########################################################################
  
  #fortify() creates zany attributes so need to reattach the values from intial dataframe
  mapa.df <- fortify(mapa)

  #creates a numeric index for each row in dataframe
  mapa@data$id <- rownames(mapa@data)

  #merge the attributes of mapa@data to the fortified dataframe with id column
  mapa.df <- join(mapa.df, mapa@data, by="id")

  ##need to do this step sometimes for some dataframes or geometry looks "torn"
  # mapa.df <- mapa.df[order(mapa.df$order),]

  
  
  #Use cut() function to divides a numeric vector into different ranges
  #note: each bin must: 1)contain a value and 2)NO records in dataframe can be null
  ###function to fill certain columns values
  getFillValues <- function(){
    # print(obj$column)
    v <- c('p_aban_imp_rfs', 'n_aban_imp_rfs', 'sed_aban_imp_rfs', 'et_aban_imp_rfs_gal', 'ratio_abandon_rfs_mlra')
    # print(match(obj$column,v))
    if(obj$column %in% v){
      print('--------match-----------------')
      mapa.df$current_field <- mapa.df$current_field * -1
      # print(mapa.df$current_field)
      return(cut(mapa.df$current_field, breaks= obj$bin_breaks))
    }
    else{
      print('-------NO-match-----------------')
      # print(cut(mapa.df$current_field, breaks= obj$bin_breaks))
      return(cut(mapa.df$current_field, breaks= obj$bin_breaks))
    }
  }

  ####create new fill column with cut values returned from getFillValues() function
  mapa.df$fill = getFillValues()

  
  #### bring in state shapefile for context in map ##################################
  state.df <- readOGR(dsn = "I:\\e_drive\\data\\usxp\\ancillary\\vector\\sf", layer = "states_wgs84")
  
  
  

  ########################################################################
  ##### create graphic ###################################################
  ########################################################################

  # There are 3 essential elements to any ggplot call:
  #
  # 1)An aesthetic that tells ggplot which variables are being mapped to the x axis, y axis, (and often other attributes of the graph, such as the color fill). Intuitively, the aesthetic can be thought of as what you are graphing.
  # 2)A geom or geometry that tells ggplot about the basic structure of the graph. Intuitively, the geom can be thought of as how you are graphing it.
  # 3)Other options, such as a graph title, axis labels and overall theme for the graph.



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


    ### county choropleth map ###########
  geom_polygon(
    data=mapa.df,
    ###Aesthetic tells ggplot which variables are being mapped to the x axis, y axis,
    ###(and often other attributes of the graph, such as the color fill).
    ###Intuitively, the aesthetic can be thought of as what you are graphing.

    ###y-axis of graph referencing lat column
    ###x-axis of graph referencing long column
    ###group tells ggplot that the data has explicit groups
    ###fill color of features referencing fill column. Fill color is initially arbitrary (changing the color of fill will be addressed later in code)
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

    #### define projection of ggplot object #######
  #### did not reproject the actual data just defined the projection of the map
  coord_map(project="polyconic") +

    #### add title to map #######
  labs(title = obj$title) +



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
      plot.title = element_text(size= 35, vjust=-12.0, hjust=0.20, color = "#4e4d47"),
      plot.caption = element_text(size= 18, color = "blue"),
      legend.position = c(0.12, -0.01)
    ) +



    ### create a discrete scale. These functions allow you to specify your own set of mappings from levels in the data to aesthetic values.
    scale_fill_manual(values = rev(brewer.pal(10, 'PRGn')[obj$legend_range]),
    # scale_fill_manual(values = custom_pallete,
                      ###legend labels
                      labels = obj$legend_labels,

                      #Legend type guide shows key (i.e., geoms) mapped onto values.
                      guide = guide_legend( title=obj$legend_title,
                                            title.theme = element_text(
                                              size = 32,
                                              color = "#4e4d47",
                                              vjust=0.0,
                                              angle = 0
                                            ),
                                            # legend bin dimensions
                                            keyheight = unit(3, units = "mm"),
                                            keywidth = unit(20, units = "mm"),

                                            #legend elements position
                                            label.position = "bottom",
                                            title.position = 'top',

                                            #The desired number of rows of legends.
                                            nrow=1

                      )
    )

return(d)

} 




