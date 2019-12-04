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





########## intact grasslands ###########################################

setwd('I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\habitat_impacts\\nlcd_intact\\tif')
r1_init = raster('intact_grasslands_2008_null_agg_3km_sum.tif')

###set max value at 2000

m <- c(0, 100, 1,  
       100, 2000, 2,  
       2000, 10000, 3)
rclmat <- matrix(m, ncol=3, byrow=TRUE)
r1 <- reclassify(r1_init, rclmat)


### convert the raster to SPDF
r1_spdf <- as(r1, "SpatialPixelsDataFrame")

###make the SPDF to a regular dataframe
r1_df <- as.data.frame(r1_spdf)


####add columns to the dataframe
colnames(r1_df) <- c("value", "x", "y")

hist(r1_df$value)

table(r1_df$value)






########## converted intact grasslands #############################

setwd('I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\habitat_impacts\\nlcd_intact\\tif')
r2_init = raster('converted_grasslands_2016_null_agg_3kmm_sum_binary.tif')

####use raster math to get the realtive ratio
r2 <- r2_init/r1_init


m <- c(0, 0.1, 10,  
       0.1, 0.2, 20,  
       0.2, 1.0, 30)
rclmat <- matrix(m, ncol=3, byrow=TRUE)
r2 <- reclassify(r2, rclmat)


### convert the raster to SPDF
r2_spdf <- as(r2, "SpatialPixelsDataFrame")

###make the SPDF to a regular dataframe
r2_df <- as.data.frame(r2_spdf)

####add columns to the dataframe
colnames(r2_df) <- c("value", "x", "y")



#####what is this !!!!!!!!!!!!?????????????
r2_df[is.na(r2_df)] <- 0
hist(r2_df$value)

table(r2_df$value)




######### merge to 2 datasets to get a raster with 9 values 
r3 = r1 + r2
unique(values(r3))

### convert the raster to SPDF
r3_spdf <- as(r3, "SpatialPixelsDataFrame")

###make the SPDF to a regular dataframe
r3_df <- as.data.frame(r3_spdf)

####add columns to the dataframe
colnames(r3_df) <- c("value", "x", "y")

####change the values of the inract reaster that have no coversion pixel associated with them to the lowest value per bin
r3_df$value[r3_df$value == 1] <- 11
r3_df$value[r3_df$value == 2] <- 21
r3_df$value[r3_df$value == 3] <- 31

hist(r3_df$value)

table(r3_df$value)



#### graphics
ggplot() + 
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
  coord_equal() +
  
  #### add title to map #######
labs(title = '') +
     # caption = '-10.0%  -7.5%   -5.0%   -2.5%                              2.5%    5.0%    7.5%    10.0%') + 
  
  
  
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
    legend.text = element_text(color='white', size=0),
    legend.position = c(0.20, 0.12),   ####(horizontal, vertical)
    legend.spacing.y = unit(-1.0, 'mm'),

    
    ####legend labels
    plot.caption = element_text(size= 25, vjust=65, hjust=0.070, color = "#4e4d47") ###title size/position/color
  ) +
  
  
###this is modifying the specifics of INSIDE the legend (i.e. the legends components that make up the legend)
####note: limits changes the order of legend items!!!!!!!!!!!
####-----red/blue----
  # scale_fill_manual( limits=c("31", "32", "33","21","22","23","11","12","13"), values = c('11'='#c9bed0','12'='#b87b8e','13'='#a8384c','21'='#8ca1c7','22'='#7f6989','23'='#73314b','31'='#5285c0','32'='#465785','33'='#3e2848'),

###---green/purple
scale_fill_manual( limits=c("31", "32", "33","21","22","23","11","12","13"), values = c('11'='#e8e8e8','12'='#b5c0da','13'='#6c83b5','21'='#b8d6be','22'='#90b2b3','23'='#567994','31'='#73ae80','32'='#5a9178','33'='#2a5a5b'),

                   
####---homemade
# scale_fill_manual( limits=c("31", "32", "33","21","22","23","11","12","13"), values = c('11'='#e8e8e8','12'='#e4acac','13'='#c85a5a','21'='#b8d6be','22'='#90b2b3','23'='#567994','31'='#73ae80','32'='#5a9178','33'='#2a5a5b'),
                   
#scale_fill_manual(values = rev(c('#89A1C8','#AE3A4E','#CABED0')),
# scale_fill_manual(values = rev(c('#276419','#4d9221','#7fbc41','#b8e186','#c2a5cf','#9970ab','#762a83','#40004b')), 
# scale_fill_manual(values = rev(c('#276419','#4d9221','#7fbc41','#b8e186','#40004b','#40004b','#40004b','#40004b')),                 

                    #Legend type guide shows key (i.e., geoms) mapped onto values.
                    guide = guide_legend( 
                                          title='fdf???',
                                          title.theme = element_text(
                                            size = 32,
                                            color = "#4e4d47",
                                            vjust=0.0,
                                            angle = 0
                                          ),
                                          
                                          # override.aes = list(colour=c('#276419','#4d9221','#7fbc41','#b8e186')),
                                          # legend bin dimensions
                                          keyheight = unit(25, units = "mm"),
                                          keywidth = unit(25, units = "mm"),
                                          
                                          #legend elements position
                                          label.position = "bottom",
                                          title.position = 'bottom',
                                          
                                          #The desired number of rows of legends.
                                          ncol = 3,
                                          byrow=TRUE
                                          
                                         
                                          
                    )
  )




fileout = 'I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\habitat_impacts\\nlcd_intact\\deliverables\\test.png'
ggsave(fileout, width = 34, height = 25, dpi = 500)
