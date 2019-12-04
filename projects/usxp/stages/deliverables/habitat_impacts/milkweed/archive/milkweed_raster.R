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
# 
# 
# 
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



# fgdb = 'I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\habitat_impacts\\milkweed\\data\\milkweed.gdb'
# mapa <- readOGR(dsn=fgdb,layer="milkweed_bs3km_region")
# 
# # crs_wgs84 = CRS('+init=EPSG:4326')
# # mapa <- spTransform(mapa, crs_wgs84)
# 
# #fortify() creates zany attributes so need to reattach the values from intial dataframe
# mapa.df <- fortify(mapa)
# 
# #creates a numeric index for each row in dataframe
# mapa@data$id <- rownames(mapa@data)
# 
# #merge the attributes of mapa@data to the fortified dataframe with id column
# mapa.df <- join(mapa.df, mapa@data, by="id")
# 
# 
# ###change the values of the ends of dataset for visual stretch purposes
# ###any value below 10 change to 10
# mapa.df$gridcode[mapa.df$gridcode<10] <- 10
# ###any value above 100,000 change t0 10,000
# mapa.df$gridcode[mapa.df$gridcode>100000] <- 100000

setwd('I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\habitat_impacts\\milkweed\\data\\tiffs')
r = raster('milkweed_bs3km_region.tif')

### convert the raster to SPDF
r_spdf <- as(r, "SpatialPixelsDataFrame")

###make the SPDF to a regular dataframe
r_df <- as.data.frame(r_spdf)

# r_df[r_df == 0] <- NA

###remove all records with zero in it
# row_sub = apply(r_df, 1, function(row) all(row !=0 ))
# r_df <- r_df[row_sub,]

####add columns to the dataframe
colnames(r_df) <- c("value", "x", "y")

# #### round value column
# r_df$value_round <-  r_df$value/100
# r_df$value_round<-round(r_df$value_round)
# 
# ##remove all rows with zero in it
# ###left side of the common is the row index and right side of the column is column index
# r_df = r_df[r_df$value_round != 0,]
# 
# ####HUGE HACK ---- Added 1 value to a (-0.5,0.5] bin for cartographic/styling purposes of the legend (wanted to create the gap between expansion and abandonment)
# r_df[1,]$value_round <- 0
# 
# ####create new fill column with cut values
# r_df$fill = cut(r_df$value_round, breaks= c(-1000000, -10, -7.5, -5, -2.5,-0.5,0.5, 2.5, 5, 7.5, 10, 100000))


#### stats
## get the counts per bin
table(r_df$fill)



### Expansion:attach df to specific object in json #####################################################
states_region = get_postgis_query(con_synthesis, "SELECT geom FROM spatial.states WHERE st_abbrev
                                           IN ('IL','IN','IA','KS','KY','MI','MN','MO','NE','ND','OH','SD','WI')",
                                                geom_name = "geom")

states_region.df <- fortify(states_region)

### Expansion:attach df to specific object in json #####################################################
states_large = get_postgis_query(con_synthesis, "SELECT geom FROM spatial.states",
                           geom_name = "geom")

states_large.df <- fortify(states_large)





###############################################
#### graphics #################################
###############################################


d <- ggplot() + 
  ### state boundary background ###########
### state boundary background ###########
geom_polygon(
  data=states_large,
  aes(x=long,y=lat,group=group),
  # fill='#D0D0D0') +
  fill='#D3D3D3') +
  
  ### state boundary strokes ###########
geom_polygon(
  data=states_large,
  aes(y=lat, x=long, group=group),
  alpha=0,
  colour='white',
  size=3
) +
  ### state boundary background ###########
  geom_polygon(
  data=states_region,
  aes(x=long,y=lat,group=group),
  # fill='#D0D0D0') +
  fill='#808080') +

  ###focus datset
  geom_tile(
    data=r_df,
    #### using alpha greatly increases the render time!!!!  --avoid if when possible
    # alpha=0.8,
    aes(x=x, y=y,fill=as.factor(value))
  ) +

  ### state boundary strokes ###########
geom_polygon(
  data=states_region,
  aes(y=lat, x=long, group=group),
  alpha=0,
  colour='white',
  size=2
) +


  # Equal scale cartesian coordinates 
coord_equal(xlim = c(-104,-80.5),ylim = c(35.84, 50)) +
#   
# coord_map(project="polyconic")
  
# coord_map(project="polyconic", xlim = c(-104,-80.5),ylim = c(35.84, 50)) + 

  #### add title to map #######
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

    plot.background = element_rect(fill = NA, color = NA),
    plot.margin = unit(c(0, 0, 0, 0), "cm"),

    #### modified attributes ########################
    ##parameters for the map title
    plot.title = element_text(size= 45, vjust=-12.0, hjust=0.10, color = "#4e4d47"),
    ##shifts the entire legend (graphic AND labels)
    legend.justification = c(0,0),
    legend.position = c(0.03, 0.02)   ####(horizontal, vertical)
    # text = element_text(color = "#4e4d47", size=30)  ##these are the legend numeric values
  ) +

  
  ###this is modifying the specifics of INSIDE the legend (i.e. the legends components that make up the legend)
  ### create a discrete scale. These functions allow you to specify your own set of mappings from levels in the data to aesthetic values.
  scale_fill_gradient(low = "#ffe3bf", 
                      high = '#f38423',
                      limits = c(10, 100000),
                      breaks = c(10, 25000, 50000, 75000, 100000),
                      labels = c('5,000', '     ', '15,000', '      ', '>25,000')
                      ) + 


  

guides(fill = guide_colorbar(title='Milkweeds Lost (stems / 10,000 acres)', reverse = FALSE, barwidth = unit(0.230, units = "npc"), barheight = unit(0.015, units = "npc"), title.position = 'top')) + theme(legend.direction = "horizontal") 



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
    ggplot_object = annotation_custom(grobTree(textGrob(labels[i], x=cnt, y= 0.01, rot = 0,gp=gpar(col="#4e4d47", fontsize=35, fontface="bold"))))
    ggplot_object_list <- append(ggplot_object_list, list(ggplot_object))
    cnt = cnt + multiplier
    i = i + 1
  }
  return(ggplot_object_list)
} 




legendlabels_abandon <- getggplotObject(cnt = 0.015, multiplier = 0.10, slots = 3, labels = c("5,000","15,000",">25,000"))

d + legendlabels_abandon

fileout = 'I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\habitat_impacts\\milkweed\\deliverables\\maps\\test.png'
ggsave(fileout, width = 34, height = 25, dpi = 500)
