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


setwd('I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\gross_net')
r = raster('tif\\s35_expand_agg3km.tif')

r = (r/10000) * 100

### convert the raster to SPDF
r_spdf <- as(r, "SpatialPixelsDataFrame")

###make the SPDF to a regular dataframe
r_df <- as.data.frame(r_spdf)

colnames(r_df) <- c("value", "x", "y")

r_df = r_df[r_df$value != 0,]
hist(r_df$value,breaks = 50)

####create new fill column with cut values
# r_df$fill = cut(r_df$value, breaks= c(0, 2.5, 5, 7.5, 10, 100000))
r_df$fill = cut(r_df$value, breaks= c(0, 2.5, 5, 7.5, 10, 100000))

#### stats
## get the counts per bin
table(r_df$fill)





#### graphics
ggplot() + 
  ### state boundary background ###########
  geom_polygon(
  data=states, 
  aes(x=long,y=lat,group=group),
  fill='#e1e1e1') +
  # fill='#808080') +

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
  coord_equal() +
  
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
    legend.text = element_text(color='white', size=0),
    # legend.position = c(0.00, 0.12),   ####(horizontal, vertical)
    legend.position = "none",   ####(horizontal, vertical)
    
    ####legend labels
    # plot.caption = element_text(size= 25, vjust=65, hjust=0.06, color = "#4e4d47") ###title size/position/color
    plot.caption = element_text(size= 25, vjust=65, hjust=0.00, color = "#4e4d47") ###title size/position/color
    
    ) +
  
  
  ###this is modifying the specifics of INSIDE the legend (i.e. the legends components that make up the legend)
  ### create a discrete scale. These functions allow you to specify your own set of mappings from levels in the data to aesthetic values.
  # scale_fill_manual(values = c('#003c30','#01665e','#35978f','#80cdc1','#c7eae5','white','#fddbc7','#f4a582','#d6604d','#b2182b','#67001f'),
  scale_fill_manual(values = c('#fddbc7','#f4a582','#d6604d','#b2182b','#67001f'),

                    #Legend type guide shows key (i.e., geoms) mapped onto values.
                    guide = guide_legend( title='Percent Expansion',
                                          title.theme = element_text(
                                            size = 32,
                                            color = "#4e4d47",
                                            vjust=0.0,
                                            angle = 0
                                          ),
                                          # legend bin dimensions
                                          keyheight = unit(5, units = "mm"),
                                          keywidth = unit(25, units = "mm"),
                                          
                                          #legend elements position
                                          label.position = "bottom",
                                          title.position = 'top',
                                          
                                          #The desired number of rows of legends.
                                          nrow=1,
                                          byrow=TRUE
                                          
                    )
  )




fileout = 'I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\gross_net\\deliverables\\test.png'
ggsave(fileout, width = 34, height = 25, dpi = 500)
