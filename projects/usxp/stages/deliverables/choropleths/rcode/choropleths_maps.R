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
library(grid)
library(scales)
library(viridis)  # better colors for everyone
library(ggthemes) # theme_map()


# root = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\temp\\R_ggplot2_map_demo_2019_06_07\\R_ggplot2_map_demo_2019_06_07\\'


custom_pallete = c('#faea05', '#f2bd0f', '#e8951a', '#d46d2c')

createMap <- function(obj){
  
  mapa <- obj$df
  
  ########################################################################
  ##### modify dataframes #####################################
  ########################################################################
  
  ### create main dataframe (mapa.df)######################################
  
  ##spatial dataframe 
  # mapa <- readOGR(dsn = "C:\\Users\\Bougie\\Box\\data_science_study_group\\resources\\R_ggplot2_map_demo_2019_06_07\\ggplot2_example_features.gdb",layer="agroibis_counties_example")
  # mapa <- spTransform(mapa, CRS("+init=epsg:5070"))
  
  ##This function turns a map into a dataframe that can more easily be plotted with ggplot2.
  mapa.df <- fortify(mapa)
  
  #fortify() creates zany attributes so need to reattach the values from intial dataframe
  #creates a numeric index for each row in dataframe
  mapa@data$id <- rownames(mapa@data)
  
  #merge the attributes of mapa@data to the fortified dataframe with id column
  mapa.df <- join(mapa.df, mapa@data, by="id")
  
  ##need to do this step sometimes for some dataframes or geometry looks "torn"
  # mapa.df <- mapa.df[order(mapa.df$order),]
  
  ##Use cut() function to divides a numeric vector into different ranges
  ##note: each bin must: 1)contain a value and 2)no records in dataframe can be null
  mapa.df$fill = cut(mapa.df$perc, breaks= c(0, 0.5, 2.5, 5, 7.5, 100))
  print (table(mapa.df$fill))
  
  
  
  
  #### bring in state shapefile for context in map ##################################
  state.df <- readOGR(dsn = "H:\\new_data_8_18_19\\e_drive\\data\\general\\sf", layer = "states")
  
  
  
  
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
    fill='#7e7e7e'
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
    
    # Equal scale cartesian coordinates
    ####NOTE: besure to set clip to off or the grob annotation is clipped by the dimensions of the panel
    coord_equal(clip = 'off') +
    
    #### add title to map #######
  labs(title = obj$legend_title) + 
    
    
    
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
      legend.position = c(0.12, 0.1)
      
    ) +
    

    
    
    ### create a discrete scale. These functions allow you to specify your own set of mappings from levels in the data to aesthetic values.
    # scale_fill_manual(values = brewer.pal(5, 'YlOrBr')[1:5], ##reference colorbrewer list of hex values
    scale_fill_manual(values = custom_pallete,       
                      ###legend labels
                      labels = "",
                      
                      #Legend type guide shows key (i.e., geoms) mapped onto values.
                      guide = guide_legend( title='',
                                            title.theme = element_text(
                                              size = 0,
                                              color = "#4e4d47",
                                              vjust=0.0,
                                              angle = 0
                                            ),
                                            # legend bin dimensions
                                            keyheight = unit(0.025, units = "npc"),
                                            keywidth = unit(0.20, units = "npc"),
                                            
                                            #legend elements position
                                            label.position = "bottom",
                                            title.position = 'top',
                                            
                                            #The desired number of rows of legends.
                                            nrow=1

                                            
                      )
    )
  
  ####add anotation to the map #####################################
  
  getggplotObject <- function(cnt, multiplier, slots, labels){
    
    ###declare the empty list that will hold all the ggplot objects
    ggplot_object_list <- list()
    
    # cnt = 0.015
    # multiplier = 0.040
    limit = cnt + (multiplier * slots)
    
    print(limit)
    
    i = 1
    # labels <- c("20%","40%","60%","80%",">80%")
    while (cnt < limit) {
      print(cnt)
      ggplot_object = annotation_custom(grobTree(textGrob(labels[i], x=cnt, y= -0.20, rot = 0,gp=gpar(col="#4e4d47", fontsize=45, fontface="bold"))))
      ggplot_object_list <- append(ggplot_object_list, list(ggplot_object))
      cnt = cnt + multiplier
      i = i + 1
    }
    return(ggplot_object_list)
    
  }

  legend_title = annotation_custom(grobTree(textGrob("Percent Expansion",  x=0.47, y= -0.05, rot = 0,gp=gpar(col="#4e4d47", fontsize=45, fontface="bold"))))
  legendlabels_abandon <- getggplotObject(cnt = 0.24, multiplier = 0.125, slots = 4, labels = c("0.5","2.5","5.0","7.5"))

  #### add annotation to map object ###################################################
  # d + legendlabels_abandon + 
  yo <- d + legendlabels_abandon + legend_title

return(yo)

} 




