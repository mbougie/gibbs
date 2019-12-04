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
# # library(ggpubr)
# library(cowplot)
# library(grid)
# library(ggthemes) # theme_map()

##### NOTE: might not need these pckages if installed in main script so delete soon!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


createMap <- function(obj){
  
  #### get datasets######################

  state.df <- readOGR(dsn = "H:\\new_data_8_18_19\\e_drive\\data\\general\\sf", layer = "states")
  
  
  
  
  
  
  
  print(obj$dataset)
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
  
  
  
  
  ####create new fill dataset with cut values returned from getFillValues() function
  # mapa.df$fill = getFillValues()
  mapa.df$fill = cut(mapa.df$current_field, breaks= obj$bin_breaks)
  
  

  
  ############ this is old junk code I think ----get rid of code soon------------------- #######################################
  #Use cut() function to divides a numeric vector into different ranges
  #note: each bin must: 1)contain a value and 2)NO records in dataframe can be null
  ###function to fill certain columns values
  # getFillValues <- function(){
  #   #### if the dataset is abandonment then flip the color scheme ####################
  #   if(grepl('abd', obj$dataset)){
  #     print('--------match-----------------')
  #     mapa.df$current_field <- mapa.df$current_field * -1
  #     
  #     return(cut(mapa.df$current_field, breaks= obj$bin_breaks))
  #   }
  #   else{
  #     print('-------NO-match-----------------')
  #     # print(cut(mapa.df$current_field, breaks= obj$bin_breaks))
  #     return(cut(mapa.df$current_field, breaks= obj$bin_breaks))
  #   }
  # }


  
  #####create stuff for the legend here (abstract this into a fct soon)
  labels = obj$bin_breaks
  # labels[labels == 1e300] <- ""
  # labels[labels == -1e300] <- ""
  # print(labels)
  
  
  
  slots = length(labels)
  print(slots)
  
  legend_length = 0.060 * slots
  print(legend_length)
  
  cnt = (1 - legend_length)/2
  print(cnt)
 
  #################################################################### 
  
  
  
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
  
  getLegendRange(obj$bin_breaks)
  

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

    
    
    # Equal scale cartesian coordinates
    ####NOTE: besure to set clip to off or the grob annotation is clipped by the dimensions of the panel
    coord_equal(clip = 'off') +
    
    #### add title to map #######
  labs(title = obj$dataset) + 
    
    
    
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
      # panel.margin=unit(-10, "cm"),
      
      
      
      
      
      
      plot.background = element_rect(fill = NA, color = NA),
      
      ###extend bottom margin of plot to accomidate legend and grob annotation
      plot.margin = unit(c(0, 0, 2, 0), "cm"),
      # plot.margin = unit(c(0, 0, 10, 0), "cm"),
      
      #### modified attributes ########################
      ##parameters for the map title
      plot.title = element_text(size= 45, vjust=-12.0, hjust=0.10, color = "#4e4d47"),
      ##shifts the entire legend (graphic AND labels)
      legend.text = element_text(color='white', size=0),
      legend.margin=margin(t = -0.1, unit='cm'),
      ###sets legend to 0,0 versus center of map???
      legend.justification = c(0,0),
      legend.position = c(0.00, -0.05),   ####(horizontal, vertical)
      ###spacing between legend bins
      # legend.spacing.x = unit(0.025, 'npc')
      legend.spacing.x = unit(1.25, 'cm')
      
      ####legend labels
      # plot.caption = element_text(size= 30, vjust=-0.9, hjust=0.070, color = "#4e4d47") ###title size/position/color
    ) +
    
    
    ###this is modifying the specifics of INSIDE the legend (i.e. the legends components that make up the legend)
    ### create a discrete scale. These functions allow you to specify your own set of mappings from levels in the data to aesthetic values.
    scale_fill_manual(values = rev(brewer.pal(10, 'PRGn')[getLegendRange(obj$bin_breaks)]),
                      
                      #Legend type guide shows key (i.e., geoms) mapped onto values.
                      guide = guide_legend( title='',
                                            title.theme = element_text(
                                              size = 0,
                                              color = "#4e4d47",
                                              vjust=0.0,
                                              angle = 0
                                            ),
                                            # legend bin dimensions
                                            # keyheight = unit(0.025, units = "npc"),
                                            # keywidth = unit(0.2, units = "npc"),
                                            
                                            keyheight = unit(0.5, units = "cm"),
                                            keywidth = unit(3, units = "cm"),
                                            
                                            #legend elements position
                                            label.position = "bottom",
                                            title.position = 'top',
                                            
                                            #The desired number of rows of legends.
                                            nrow=1
                                            # byrow=TRUE
                                            
                      )
    )
  
  
  
  getggplotObject <- function(cnt, multiplier, slots, labels){
    
    ###declare the empty list that will hold all the ggplot objects
    ggplot_object_list <- list()
    
    # length_of_legend = (multiplier * slots)
    # print(length_of_legend)
    
    limit = cnt + (multiplier * slots)
    print(limit)
    
    i = 1
    # labels <- c("20%","40%","60%","80%",">80%")
    while (cnt < limit) {
      print(cnt)
      ggplot_object = annotation_custom(grobTree(textGrob(labels[i], x=cnt, y= -0.06, just="left", rot = -45,gp=gpar(col="#4e4d47", fontsize=45, fontface="bold"))))
      ggplot_object_list <- append(ggplot_object_list, list(ggplot_object))
      cnt = cnt + multiplier
      i = i + 1
    }
    return(ggplot_object_list)
    
  }
  
  
  
  
  
  
  legend_title <- annotation_custom(grobTree(textGrob(obj$legend_title, x = 0.00, y = 0.025, just="left", rot = 0,gp=gpar(col="#4e4d47", fontsize=45, fontface="bold"))))
  legendlabels <- getggplotObject(cnt = 0, multiplier = 0.083, slots = length(labels), labels = labels)
  
  
  
  #### add annotation to map object ###################################################
  # d + legendlabels_abandon + 
  ggplot_obj = d + legend_title + legendlabels
  

return(ggplot_obj)

} 




