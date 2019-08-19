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
# ### Make the connection to database ######################################################################
# con_synthesis <- dbConnect(PostgreSQL(), dbname = 'usxp_deliverables', user = user, host = host, port=port, password = password)
# 
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
# setwd('I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\gross_net\\tiffs')
# r = raster('s35_3kmagg_net.tif')
# 
# ### convert the raster to SPDF
# r_spdf <- as(r, "SpatialPixelsDataFrame")
# 
# ###make the SPDF to a regular dataframe
# r_df <- as.data.frame(r_spdf)
# 
# # r_df[r_df == 0] <- NA
# 
# ###remove all records with zero in it
# # row_sub = apply(r_df, 1, function(row) all(row !=0 ))
# # r_df <- r_df[row_sub,]
# 
# ####add columns to the dataframe
# colnames(r_df) <- c("value", "x", "y")
# 
# #### round value column
# r_df$value_round <-  r_df$value/100
# r_df$value_round<-round(r_df$value_round)
# 
# ##remove all rows with zero in it
# ###left side of the common is the row index and right side of the column is column index
# r_df = r_df[r_df$value_round != 0,]
# 
# ####create new fill column with cut values
# r_df$fill = cut(r_df$value_round, breaks= c(-1000000, -7.5, -5, -2.5,-0.5,0.5, 2.5, 5, 7.5, 100000))


#### stats
## get the counts per bin
table(r_df$fill)

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
labs(title = 'Intact compare SDSU',
     caption = '   <-7.5   -7.5    -5.0    -2.5   2.5    5.0   7.5    >7.5') + 
  
  
  
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
    plot.caption = element_text(size= 30, vjust=-10, hjust=0.09, color = "#4e4d47"), ###title size/position/color
    legend.position = c(0.25, -0.00),   ####h,V
    text = element_text(color = "#4e4d47", size=30)  ##these are the legend numeric values
  ) +
  
  
  ###this is modifying the specifics of INSIDE the legend (i.e. the legends components that make up the legend)
  ### create a discrete scale. These functions allow you to specify your own set of mappings from levels in the data to aesthetic values.
  scale_fill_manual(values = c('#00441b','#1b7837','#5aae61','#a6dba0','#f4a582','#d6604d','#b2182b','#67001f'),
                    # scale_fill_manual(values = custom_pallete,
                    ###legend labels
                    # labels = c('<-7.5','-7.5','-5.0','-2.5','2.5','5.0','7.5','>7.5'),
                    
                    #Legend type guide shows key (i.e., geoms) mapped onto values.
                    guide = guide_legend( title='legend title',
                                          title.theme = element_text(
                                            size = 32,
                                            color = "#4e4d47",
                                            vjust=0.0,
                                            angle = 0
                                          ),
                                          # legend bin dimensions
                                          keyheight = unit(8, units = "mm"),
                                          keywidth = unit(35, units = "mm"),
                                          
                                          #legend elements position
                                          label.position = "bottom",
                                          title.position = 'top',
                                          
                                          #The desired number of rows of legends.
                                          nrow=1
                                          
                    )
  )




fileout = 'I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\gross_net\\images\\s35_bs3km_net_png500_r_no_alpha2.png'
ggsave(fileout, width = 34, height = 25, dpi = 500)
