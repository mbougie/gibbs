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
library(egg)  #### allows size of panle to change
library(scales)
library(viridis)  # better colors for everyone
library(ggthemes) # theme_map()




setwd('I:\\test\\irrigation\\irrigation-20191228T191036Z-001\\irrigation')
r = raster('irrigation-0000046592-0000046592.tif')



layer1 <- system.file("irrigation-0000046592-0000046592.tif", package="gdalUtils")
layer2 <- system.file("irrigation-0000093184-0000000000.tif", package="gdalUtils")
mosaic_rasters(gdalfile=c(layer1,layer2),dst_dataset="test_mosaic.envi",separate=TRUE,of="ENVI",
               verbose=TRUE)

### convert the raster to SPDF
r_spdf <- as(r, "SpatialPixelsDataFrame")

###make the SPDF to a regular dataframe
r_df <- as.data.frame(r_spdf)

####add columns to the dataframe
colnames(r_df) <- c("value", "x", "y")

#### round value column
r_df$value_round <-  r_df$value/100
r_df$value_round<-round(r_df$value_round)

##remove all rows with zero in it
###left side of the common is the row index and right side of the column is column index
r_df = r_df[r_df$value_round != 0,]

####HUGE HACK ---- Added 1 value to a (-0.5,0.5] bin for cartographic/styling purposes of the legend (wanted to create the gap between expansion and abandonment)
r_df[1,]$value_round <- 0

####create new fill column with cut values
r_df$fill = cut(r_df$value_round, breaks= c(-1000000, -10, -7.5, -5, -2.5,-0.5,0.5, 2.5, 5, 7.5, 10, 100000))


#### stats
## get the counts per bin
table(r_df$fill)





#### graphics
d <- ggplot() + 
  ### state boundary background ###########
geom_polygon(
  data=states, 
  aes(x=long,y=lat,group=group),
  # fill='#D0D0D0') +
  fill='#808080') +
  
  ###focus datset
  geom_tile(
    data=r_df,
    #### using alpha greatly increases the render time!!!!  --avoid if when possible
    # alpha=0.8,
    aes(x=x, y=y,fill=fill)
  ) +
  
  
  ### state boundary strokes ###########
geom_polygon(
  data=states,
  aes(y=lat, x=long, group=group),
  # alpha=0,
  fill=NA,
  colour='white',
  size=2
) +
  
  
  
  # Equal scale cartesian coordinates
  ####NOTE: besure to set clip to off or the grob annotation is clipped by the dimensions of the panel
  coord_equal(clip = 'off') +
  
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
    # legend.position = c(0.025, 0.12),   ####(horizontal, vertical)
    ####establishes the 0,0, origin!!!!
    # legend.background = element_rect(fill="lightblue", 
    #                                        size=0.5, linetype="solid"), 
    
    ###sets legend to 0,0 versus center of map???
    legend.justification = c(0,0),
    legend.position = c(0.13, -0.05),   ####(horizontal, vertical)
    ###spacing between legend bins
    legend.spacing.x = unit(0.5, 'cm')
    
    ####legend labels
    # plot.caption = element_text(size= 30, vjust=-0.9, hjust=0.070, color = "#4e4d47") ###title size/position/color
  ) +
  
  
  ###this is modifying the specifics of INSIDE the legend (i.e. the legends components that make up the legend)
  ### create a discrete scale. These functions allow you to specify your own set of mappings from levels in the data to aesthetic values.
  scale_fill_manual(values = c('#003c30','#01665e','#35978f','#80cdc1','#c7eae5','white','#fddbc7','#f4a582','#d6604d','#b2182b','#67001f'),
                    
                    #Legend type guide shows key (i.e., geoms) mapped onto values.
                    guide = guide_legend( title='',
                                          title.theme = element_text(
                                            size = 0,
                                            color = "#4e4d47",
                                            vjust=0.0,
                                            angle = 0
                                          ),
                                          # legend bin dimensions
                                          keyheight = unit(0.015, units = "npc"),
                                          keywidth = unit(0.055, units = "npc"),
                                          
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
  
  # cnt = 0.015
  # multiplier = 0.040
  limit = cnt + (multiplier * slots)
  
  print(limit)
  
  i = 1
  # labels <- c("20%","40%","60%","80%",">80%")
  while (cnt < limit) {
    print(cnt)
    ggplot_object = annotation_custom(grobTree(textGrob(labels[i], x=cnt, y= -0.07, rot = 0,gp=gpar(col="#4e4d47", fontsize=45, fontface="bold"))))
    ggplot_object_list <- append(ggplot_object_list, list(ggplot_object))
    cnt = cnt + multiplier
    i = i + 1
  }
  return(ggplot_object_list)
  
}







legend_title_abandon = annotation_custom(grobTree(textGrob("% Abandonment", x = 0.29, y = 0.0, rot = 0,gp=gpar(col="#4e4d47", fontsize=45, fontface="bold"))))
legendlabels_abandon <- getggplotObject(cnt = 0.188, multiplier = 0.060, slots = 5, labels = c("-10.0","-7.5","-5.0","-2.5","-0.5"))

legend_title_expand = annotation_custom(grobTree(textGrob("% Expansion", x = 0.66, y = 0.0, rot = 0,gp=gpar(col="#4e4d47", fontsize=45, fontface="bold"))))
legendlabels_expand <- getggplotObject(cnt = 0.500, multiplier = 0.060, slots = 5, labels = c("0.5","2.5","5.0","7.5","10.0"))

#### add annotation to map object ###################################################
# d + legendlabels_abandon + 
d + legend_title_abandon + legendlabels_abandon +legend_title_expand + legendlabels_expand


# yo <- set_panel_size(d,
#                    width  = unit(30, "in"),
#                    height = unit(15, "in"))
# grid.newpage()
# grid.draw(yo)


fileout = 'I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\gross_net\\deliverables\\test.png'
# ggsave(fileout, width = 40, height = 25, dpi = 500, yo, limitsize = FALSE)
ggsave(fileout, width = 34, height = 25, dpi = 500)

