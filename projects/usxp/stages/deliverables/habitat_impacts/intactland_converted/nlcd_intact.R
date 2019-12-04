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


setwd('I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\habitat_impacts\\nlcd_intact\\tif')
r1_init = raster('intact_grasslands_2008_null_agg_3km_sum.tif')

###convert to percent realtive to number of pixels in a 3km block
r1 <- (r1_init/10000) * 100


r1[r1 < 1] <- NA




m <- c(0, 5, 1,  
       1, 10, 2,  
       10, 20, 3,
       20, 40, 4,
       40, 100, 5)
rclmat <- matrix(m, ncol=3, byrow=TRUE)
r1 <- reclassify(r1, rclmat)

### convert the raster to SPDF
r1_spdf <- as(r1, "SpatialPixelsDataFrame")

###make the SPDF to a regular dataframe
r1_df <- as.data.frame(r1_spdf)

####add columns to the dataframe
colnames(r1_df) <- c("value", "x", "y")

hist(r1_df$value,breaks = 50)
table(r1_df$value)



test <- r1_df
hist(test$value,breaks = 50)

test <- subset(test, value >= 1)
hist(test$value,breaks = 50)

# test$value[test$value > 75] <- 50
# hist(test$value,breaks = 50)


hist(test$value,breaks = 50)
table(test$value)







##### RELATIVE ##########
########## converted intact grasslands #############################

setwd('I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\habitat_impacts\\nlcd_intact\\tif')
r2_init = raster('converted_grasslands_2016_null_agg_3kmm_sum_binary.tif')

####use raster math to get the realtive ratio
r2 <- (r2_init/r1_init)*100


m <- c(0, 2, 10,  
       2, 4, 20,  
       4, 8, 30,
       8, 100, 40)
rclmat <- matrix(m, ncol=3, byrow=TRUE)
r2 <- reclassify(r2, rclmat)


# r2_df$fill = cut(r2_df$perc, breaks= c(0,2,4,8,100))

### convert the raster to SPDF
r2_spdf <- as(r2, "SpatialPixelsDataFrame")

###make the SPDF to a regular dataframe
r2_df <- as.data.frame(r2_spdf)

####add columns to the dataframe
colnames(r2_df) <- c("value", "x", "y")

hist(r2_df$value,breaks = 50)

##remove all rows with zero in it
r2_df <- subset(r2_df, value != 0)


test <- r2_df 
hist(test$value,breaks = 50)


##remove all rows with zero in it
test <- subset(test, value != 0)

test$value[test$value>25] <- 25
hist(test$value,breaks = 50)


hist(r2_df$value,breaks = 50)


table(r2_df$value)




##### ABSOLUTE ##########
########## converted intact grasslands #############################

setwd('I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\habitat_impacts\\nlcd_intact\\tif')
r2_init = raster('converted_grasslands_2016_null_agg_3kmm_sum_binary.tif')

####use raster math to get the realtive ratio
r2 <- (r2_init/10000)*100


m <- c(0, 1, 10,
       1, 2.5, 20,
       2.5, 5, 30,
       5, 10, 40,
       10, 100, 50)
rclmat <- matrix(m, ncol=3, byrow=TRUE)
r2 <- reclassify(r2, rclmat)



# m <- c(0, 0.5, 10,
#        0.5, 1, 20,
#        1, 2, 30,
#        2,4, 40,
#        4, 100, 50)
# rclmat <- matrix(m, ncol=3, byrow=TRUE)
# r2 <- reclassify(r2, rclmat)



### convert the raster to SPDF
r2_spdf <- as(r2, "SpatialPixelsDataFrame")

###make the SPDF to a regular dataframe
r2_df <- as.data.frame(r2_spdf)

####add columns to the dataframe
colnames(r2_df) <- c("value", "x", "y")


hist(r2_df$value,breaks = 50)

##remove all rows with zero in it
# r2_df <- subset(r2_df, value != 0)
# hist(r2_df$value,breaks = 50)


####-----  use test  --------------------------
test <- r2_df 
hist(test$value,breaks = 50)


##remove all rows with zero in it
test <- subset(test, value != 0)

test$value[test$value>25] <- 25
hist(test$value,breaks = 50)
####--------------------------------------------

hist(r2_df$value,breaks = 50)


table(r2_df$value)



#### graphics
ggplot() + 
  ### state boundary background ###########
  geom_polygon(
  data=states, 
  aes(x=long,y=lat,group=group),
  # fill='#D0D0D0') +
  fill='#808080') +

  ##focus datset
  geom_tile(
  data=r1_df,
  #### using alpha greatly increases the render time!!!!  --avoid if when possible
  # alpha=0.8,
  aes(x=x, y=y,fill=as.factor(value))
  ) +
  
  
  ###focus datset
  geom_tile(
    data=r2_df,
    #### using alpha greatly increases the render time!!!!  --avoid if when possible
    # alpha=0.8,
    aes(x=x, y=y,fill=as.factor(value))
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

    
    ####legend labels
    plot.caption = element_text(size= 25, vjust=65, hjust=0.070, color = "#4e4d47") ###title size/position/color
  ) +
  
  
  ###this is modifying the specifics of INSIDE the legend (i.e. the legends components that make up the legend)
  ### create a discrete scale. These functions allow you to specify your own set of mappings from levels in the data to aesthetic values.
  # scale_fill_manual(values = rev(c('#276419','#4d9221','#7fbc41','cyan','blue','#fb6a4a','#fcae91','yellow')),
  scale_fill_manual( limits=c("10","20","30","40", "50", "1", "2", "3", "4", "5"), values = c('1'='#e6f5d0','2'='#b8e186','3'='#7fbc41','4'='#4d9221','5'='#276419','10'='#fddbc7','20'='#f4a582','30'='#d6604d','40'='#b2182b', '50'='#67001f'),
  ###green scale                  
  # scale_fill_manual(values = rev(c('#276419','#4d9221','#7fbc41','#b8e186','#e6f5d0')),
  
  ####red scale
  # scale_fill_manual(values = c('10'='#fee5d9','20'='#fcae91','30'='#fb6a4a','40'='#cb181d'),
                  

                                 

                    #Legend type guide shows key (i.e., geoms) mapped onto values.
                    guide = guide_legend( 
                                          # title='             Abandonment                                       Expansion',
                                          title.theme = element_text(
                                            size = 32,
                                            color = "#4e4d47",
                                            vjust=0.0,
                                            angle = 0
                                          ),
                                          
                                          # override.aes = list(colour=c('#276419','#4d9221','#7fbc41','#b8e186')),
                                          # legend bin dimensions
                                          keyheight = unit(5, units = "mm"),
                                          keywidth = unit(25, units = "mm"),
                                          
                                          #legend elements position
                                          label.position = "bottom",
                                          title.position = 'top',
                                          
                                          #The desired number of rows of legends.
                                          nrow=2,
                                          byrow=TRUE
                                          
                    )
  )




fileout = 'I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\habitat_impacts\\nlcd_intact\\deliverables\\test.png'
ggsave(fileout, width = 34, height = 25, dpi = 500)
