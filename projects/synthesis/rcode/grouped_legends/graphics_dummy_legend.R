library(ggplot2)
library(maps)
library(rgdal)# R wrapper around GDAL/OGR
library(sp)
require("RPostgreSQL")
library(plyr)
library(dplyr)
# library(viridis)
library(scales)
require(RColorBrewer)
library(glue)
library(gridBase)
library(grid)
library(gridExtra) #load Grid


createGroupLegend <- function(figure_params, col){
  
  print(col)
  gap=figure_params$gap
  print(gap)
  keywidth=figure_params$keywidth
  print(keywidth)
  keyheight=figure_params$keyheight
  print(keyheight)
  palette=figure_params$palette
  print(palette)
  
  bin_breaks = figure_params$columns[[col]]$args$bin_breaks
  print(bin_breaks)
  legend_title = figure_params$columns[[col]]$args$legend_title
  print(legend_title)
  
  
  ########## seth's code (comment soon!!!!!!!) ########################
  
  onePrior = function(x){
    # print('----------------x-----------------------------')
    # print(x)
    sign = x/abs(x)
    # print('sign')
    # print(sign)
    if(is.nan(sign)){
      # print('null sign so return:')
      # print(x)
      return(x)
    }else{
      # print('NOT null sign so return:')
      # print(sign*(abs(x)-1))
      return(sign*(abs(x)-1))
    }
  }
  
  vec = sapply(bin_breaks, onePrior)
  print('vec')
  print(vec)
  df = data.frame(cbind(x = vec, y = vec))
  
  df['cuts'] = cut(df$x, breaks= bin_breaks)
  df = df[which(df$x != 0),]  # JANKALERT
  
  print(df)
  
  ########################################################################
  
  
  
  
  
  #### function to get legend range vectro for color pallette that is derived from breaks vector in json
  
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
  
  
  ##### arguments for the graphics #########################
  number_of_breaks = length(bin_breaks)
  print('----------number_of_breaks--------------')
  print(number_of_breaks)
  
  legend_length = (keywidth * (number_of_breaks)) + (gap*(number_of_breaks-4))
  print('---------legend_length-----------')
  print(legend_length)
  
  ####get the location of where the legend start postion is
  start_legend = (1 - (legend_length))/2
  print('===================start_legend=============================')
  print(start_legend)
  
  
  
  
  ##### arguments for the labels #########################
  ###get labels
  
  print('bin_breaks')
  print(bin_breaks)
  remove <- c (-100000, 100000)
  labels = bin_breaks[!bin_breaks %in% remove]
  labels = comma(labels)
  
  ###get the number of labels
  number_of_labels = length(labels)
  print('---------number_of_labels-----------')
  print(number_of_labels)
  
  ###get the number of gaps between the labels
  gap_count = number_of_labels-1
  print('---------gap_count-----------')
  print(gap_count)
  
  ###get the total label length
  label_length = keywidth * gap_count
  print('--------label_length-----------')
  print(label_length)
  
  
  ### add keywidth plus a full gap (need to account for gap on both sides?!?)
  start_label = start_legend + keywidth + gap
  print('---------------start_label------------------')
  print(start_label)
  
  
  
  
  legendLabelCreator <- function(start_label, number_of_breaks, labels){
    
    ###declare the empty list that will hold all the ggplot objects
    ggplot_object_list <- list()
    
    i = 1
    while (i <= number_of_labels) {
      print('start_label---------------')
      print(start_label)
      
      ####create the grob-object
      ggplot_object = annotation_custom(grobTree(textGrob(labels[i], x=start_label, y= 0.45, just="left", rot = -45,gp=gpar(col="#4e4d47", fontsize=45, fontface="bold"))))
      
      ####append the grob-object to the ggplot_object_list
      ggplot_object_list <- append(ggplot_object_list, list(ggplot_object))
      
      ### add the keywidth and gap (acount for both sides of the key) to the location value to move foward with next bin
      start_label = start_label + keywidth + gap
      
      i = i + 1
      
    }
    return(ggplot_object_list)
    
  }
  
  
  
  
  
  
  
  ########################################################################
  ##### create graphic ###################################################
  ########################################################################
  
  d = ggplot(df, aes(x = x, y = y, fill = cuts)) +
    geom_area() +
    
    theme(
      #### axis attributes ##################
      axis.text.x = element_blank(),
      axis.title.x=element_blank(),
      axis.text.y = element_blank(),
      axis.title.y=element_blank(),
      axis.ticks = element_blank(),
      axis.line = element_blank(),
      
      ######plot attributes #####################
      panel.background = element_rect(fill = NA, color = NA),
      panel.grid.major = element_blank(),
      # panel.margin=unit(-10, "cm"),
      
      ######plot attributes ################################
      plot.background = element_rect(fill = NA, color = NA),
      ###extend bottom margin of plot to accomidate legend and grob annotation
      plot.margin = unit(c(0, 0, 2, 0), "cm"),
      
      ######legend attributes ###############################
      legend.title = element_blank(),
      #### modified attributes ########################
      legend.text = element_blank(),
      ###sets legend to 0,0 versus center of map???
      legend.justification = c(0,0),
      legend.position = c(start_legend, 0.5),   ####(horizontal, vertical)
      ###spacing between legend number_of_breaks
      legend.spacing.x = unit(gap, 'npc')
      
    ) +
    
    scale_fill_manual(values = rev(brewer.pal(10, palette)[getLegendRange(bin_breaks)]),
                      
                      
                      
                      #Legend type guide shows key (i.e., geoms) mapped onto values.
                      guide = guide_legend(
                        # legend bin dimensions
                        
                        keywidth = unit(2.975*keywidth, units = "npc"),
                        keyheight = unit(keyheight, units = "npc"),
                        
                        #legend elements position
                        label.position = "bottom",
                        title.position = 'top',
                        
                        #The desired number of rows of legends.
                        nrow=1
                      )
    )
  
  
  
  
  legend_title <- annotation_custom(grobTree(textGrob(legend_title, x = start_legend, y = 0.7, just="left", rot = 0,gp=gpar(col="#4e4d47", fontsize=45, fontface="bold"))))
  legendlabels <- legendLabelCreator(start_label = start_label, number_of_breaks = number_of_breaks, labels = labels)
  
  
  #### add annotation to map object ###################################################
  ggplot_obj = d + legend_title + legendlabels
  return(ggplot_obj)
}


