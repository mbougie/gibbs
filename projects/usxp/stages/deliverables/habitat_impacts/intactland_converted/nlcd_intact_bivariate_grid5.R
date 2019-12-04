# rm(list = ls())
# 
# require(ggplot2)
# require(rgdal)
# require(sp)
# require(raster)
# require(RColorBrewer)
# require(ggpubr)
# require("RPostgreSQL")
# library(postGIStools)
# require(sqldf)
# 
# 
# 
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
# 
# 
# ### Make the connection to database ######################################################################
# con_synthesis <- dbConnect(PostgreSQL(), dbname = 'usxp_deliverables', user = user, host = host, port=port, password = password)
# 
# 
# 
# setwd('I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\habitat_impacts\\grassland_conversion\\tif')
# 
# 
# 
# 
# 
# 
# 
# 
# 
# ### Expansion:attach df to specific object in json #####################################################
# states = get_postgis_query(con_synthesis, "SELECT * FROM spatial.states",
#                                                 geom_name = "geom")
# 
# 
# 
# 
# 
# ########## intact grasslands ###########################################
# r1_init = raster('intact_grasslands_2008_null_agg_3km_sum.tif')
# r1 = r1_init
# 
# ####convert the null to a very small number so no pixels are removed when perform raster math addition in r3
# r1_init[is.na(r1_init)] <- 0.01
# 
# 
# ###convert to percent realtive to number of pixels in a 3km block
# r1 <- (r1_init/10000) * 100
# 
# # r1[r1 < 1] <- NA
# 
# ##### reclassify ###############################
# m <- c(0.00001, 1.0, 0,
#        1.0, 5, 1,
#        5, 10, 2,
#        10, 20, 3,
#        20, 40, 4,
#        40, 100, 5)
# rclmat <- matrix(m, ncol=3, byrow=TRUE)
# r1 <- reclassify(r1, rclmat)
# ###############################################
# 
# ### convert the raster to SPDF
# r1_spdf <- as(r1, "SpatialPixelsDataFrame")
# 
# ###make the SPDF to a regular dataframe
# r1_df <- as.data.frame(r1_spdf)
# 
# ####add columns to the dataframe
# colnames(r1_df) <- c("value", "x", "y")
# 
# hist(r1_df$value,breaks = 50)
# table(r1_df$value)
# 
# 
# 
# 
# ########## converted intact grasslands #############################
# ##### RELATIVE ##########
# ########## converted intact grasslands #############################
# r2_init = raster('s35_intact_grassland_converted_agg_3km_sum.tif')
# r2 = r2_init
# 
# # ####convert the null to a very small number so no pixels are removed when perform raster math addition in r3
# r2_init[is.na(r2_init)] <- 0
# 
# ####use raster math to get the realtive ratio
# r2 <- (r2_init/r1_init)*100
# 
# ######### reclassify into groups #######################
# m <- c(0, 2.5, 10,
#        2.5, 5, 20,
#        5, 10, 30,
#        10, 20, 40,
#        20, 10000000000000000, 50)   ####weird final value because of the issues of intactlands and conversion not being completely compatable datasets
# rclmat <- matrix(m, ncol=3, byrow=TRUE)
# r2 <- reclassify(r2, rclmat)
# ########################################################
# 
# ### convert the raster to SPDF
# r2_spdf <- as(r2, "SpatialPixelsDataFrame")
# 
# ###make the SPDF to a regular dataframe
# r2_df <- as.data.frame(r2_spdf)
# 
# ####add columns to the dataframe
# colnames(r2_df) <- c("value", "x", "y")
# 
# hist(r2_df$value,breaks = 50)
# 
# ##remove all rows with zero in it
# # r2_df <- subset(r2_df, value != 0)
# 
# 
# ####----test-----------------------
# test <- r2_df
# hist(test$value,breaks = 50)
# 
# ##remove all rows with zero in it
# test <- subset(test, value != 0)
# 
# test$value[test$value>25] <- 25
# hist(test$value,breaks = 50)
# ####-------------------------------
# 
# hist(r2_df$value,breaks = 50)
# 
# 
# table(r2_df$value)
# 
# 
# 
# 
# 
# 
# ######### merge to 2 datasets to get a raster with 9 values
# r3 = r1 + r2
# unique(values(r3))
# 
# ### convert the raster to SPDF
# r3_spdf <- as(r3, "SpatialPixelsDataFrame")
# 
# ###make the SPDF to a regular dataframe
# r3_df <- as.data.frame(r3_spdf)
# 
# ####add columns to the dataframe
# colnames(r3_df) <- c("value", "x", "y")
# 
# # ####change the values of the inract reaster that have no coversion pixel associated with them to the lowest value per bin
# r3_df$value[r3_df$value == 0] <- NA   #### get rid of this because this is 0 from reclass and
# r3_df$value[r3_df$value == 1] <- 11
# r3_df$value[r3_df$value == 2] <- 21
# r3_df$value[r3_df$value == 3] <- 31
# r3_df$value[r3_df$value == 4] <- 41
# r3_df$value[r3_df$value == 5] <- 51
# 
# hist(r3_df$value,breaks = 50)
# 
# table(r3_df$value)









######################################################################################
#### graphics  #######################################################################
######################################################################################


d <- ggplot() + 
  ### state boundary background ###########
  geom_polygon(
  data=states, 
  aes(x=long,y=lat,group=group),
  # fill='#D0D0D0') +
  fill='#808080') +

  ###focus datset
  geom_tile(
  data=r3_df,
  #### using alpha greatly increases the render time!!!!  --avoid if when possible
  # alpha=0.8,
  aes(x=x, y=y,fill=as.factor(value))
  ) +
  
  
  # ###focus datset
  # geom_tile(
  #   data=r2_df,
  #   #### using alpha greatly increases the render time!!!!  --avoid if when possible
  #   # alpha=0.8,
  #   aes(x=x, y=y,fill=fill)
  # ) +
  
  
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
labs(tag = '',
     caption = '') + 
  
  
  
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
    plot.margin = unit(c(0, 0, 0, 0), "cm"),
    
    #### modified attributes ########################
    ##shifts the entire legend (graphic AND labels)
    legend.text = element_text(color='white', size=0),
    legend.justification = c(0,0),
    legend.position = c(0.05, 0.05),   ####(horizontal, vertical)
    legend.spacing.y = unit(-1.0, 'mm')
    

    
    ####legend labels (hacked to be used as labels for legend)
    # plot.tag = element_text(size = 30, vjust = 5, hjust = 5, color = "#4e4d47", angle = 90) ### x-axis label of legend
  ) +
  
  
###this is modifying the specifics of INSIDE the legend (i.e. the legends components that make up the legend)
####note: limits changes the order of legend items!!!!!!!!!!!
####-----red/blue----
  # scale_fill_manual( limits=c("31", "32", "33","21","22","23","11","12","13"), values = c('11'='#c9bed0','12'='#b87b8e','13'='#a8384c','21'='#8ca1c7','22'='#7f6989','23'='#73314b','31'='#5285c0','32'='#465785','33'='#3e2848'),

###---green/purple
scale_fill_manual( limits=c("51", "52", "53","54","55","41", "42", "43","44","45","31", "32", 
                            "33","34","35","21","22","23","24","25","11","12","13","14","15"), 
                   
                   values = c('11'='#fefebf','12'='#fee897','13'='#fad75b','14'='#fdbe40','15'='#ffa807',
                              '21'='#cfebba','22'='#d4cb8c','23'='#d8b360','24'='#d99e36','25'='#db7e06',
                              '31'='#9fd4b6','32'='#acb587','33'='#b39655','34'='#b4812b','35'='#b25209',
                              '41'='#6bc4ae','42'='#87977c','43'='#947b4e','44'='#98542a','45'='#8c340e',
                              '51'='#37afac','52'='#62846f','53'='#755f44','54'='#774c1f','55'='#730001'),

                   
####---homemade
# scale_fill_manual( limits=c("31", "32", "33","21","22","23","11","12","13"), values = c('11'='#e8e8e8','12'='#e4acac','13'='#c85a5a','21'='#b8d6be','22'='#90b2b3','23'='#567994','31'='#73ae80','32'='#5a9178','33'='#2a5a5b'),
                   
#scale_fill_manual(values = rev(c('#89A1C8','#AE3A4E','#CABED0')),
# scale_fill_manual(values = rev(c('#276419','#4d9221','#7fbc41','#b8e186','#c2a5cf','#9970ab','#762a83','#40004b')), 
# scale_fill_manual(values = rev(c('#276419','#4d9221','#7fbc41','#b8e186','#40004b','#40004b','#40004b','#40004b')), 





                    #Legend type guide shows key (i.e., geoms) mapped onto values.
                    guide = guide_legend( 
                                          title='',
                                          title.theme = element_text(
                                            size = 32,
                                            color = "#4e4d47",
                                            vjust=0,
                                            angle = 0
                                          ),
                                          
                                          
                                 
                                          
                                          # override.aes = list(colour=c('#276419','#4d9221','#7fbc41','#b8e186')),
                                          # legend bin dimensions
                                          keyheight = unit(25, units = "mm"),
                                          keywidth = unit(25, units = "mm"),
                                          
                                          #legend elements position
                                          direction = "horizontal",
                                          label.position = "bottom",
                                          title.position = 'left',
                                          
                                          #The desired number of rows of legends.
                                          ncol = 5,
                                          byrow=TRUE
                                          
                                         
                                          
                    )
  )


getggplotObject_x <- function(x, y, multiplier, slots, labels){
  
  ###declare the empty list that will hold all the ggplot objects
  ggplot_object_list <- list()
  
  # cnt = 0.015
  # multiplier = 0.040
  limit = x + (multiplier * slots)
  
  print(limit)
  
  i = 1
  # labels <- c("20%","40%","60%","80%",">80%")
  while (x < limit) {
    print(x)
    ggplot_object = annotation_custom(grobTree(textGrob(labels[i], x=x, y= y, rot = 0,gp=gpar(col="#4e4d47", fontsize=45, fontface="bold"))))
    ggplot_object_list <- append(ggplot_object_list, list(ggplot_object))
    x = x + multiplier
    i = i + 1
  }
  return(ggplot_object_list)
  
}




getggplotObject_y <- function(x, y, multiplier, slots, labels){
  
  ###declare the empty list that will hold all the ggplot objects
  ggplot_object_list <- list()
  
  # cnt = 0.015
  # multiplier = 0.040
  limit = y + (multiplier * slots)
  
  print(limit)
  
  i = 1
  # labels <- c("20%","40%","60%","80%",">80%")
  while (y <= limit) {
    print(y)
    ggplot_object = annotation_custom(grobTree(textGrob(labels[i], x=x, y= y, rot = 0,gp=gpar(col="#4e4d47", fontsize=45, fontface="bold"))))
    ggplot_object_list <- append(ggplot_object_list, list(ggplot_object))
    y = y + multiplier
    i = i + 1
  }
  return(ggplot_object_list)
  
}


############ add legend annotation to map object #######################################################

##### INTACT Y-AXIS legend ################################################################
legend_y_numeric <- getggplotObject_y(x = 0.045, y=0.060, multiplier = 0.045, slots = 4, labels = c("1","5","10","20","40"))
legend_y = annotation_custom(grobTree(textGrob("% Intact \U2192",  x=0.020, y= 0.16, rot = 90,gp=gpar(col="#4e4d47", fontsize=50, fontface="bold"))))



##### CONVERSION X-AXIS legend #################################################################
legend_x_numeric <- getggplotObject_x(x = 0.0525, y=0.03, multiplier = 0.0322, slots = 5, labels = c("0","2.5","5","10","20"))
legend_x = annotation_custom(grobTree(textGrob("% Converted \U2192", x=0.135, y= -0.01, rot = 0,gp=gpar(col="#4e4d47", fontsize=50, fontface="bold"))))




#### add annotation to map object ###################################################
d + legend_x + legend_x_numeric + legend_y + legend_y_numeric

fileout = 'I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\habitat_impacts\\grassland_conversion\\deliverables\\test.png'
ggsave(fileout, width = 34, height = 25, dpi = 500)
