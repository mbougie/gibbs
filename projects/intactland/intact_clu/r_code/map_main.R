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
con <- dbConnect(PostgreSQL(), dbname = 'intactland', user = user, host = host, port=port, password = password)



##### query the data from postgreSQL
hex_colors <- dbGetQuery(con,"
                              SELECT
                              label,
                              new_value as value,
                              hex,
                              CASE
                              when label = 'nodata_np' then 9
                              when label = 'nodata_p' then 10
                              when label = 'non_intact_np' then 11
                              when label = 'non_intact_p' then 12
                              END hex_order
                              FROM 
                              intact_clu.combined
                              WHERE hex IS NOT NULL
                              
                              UNION
                              
                              SELECT
                              label,
                              new_value as value,
                              hex,
                              CASE
                              when label = 'grassland_np' then 1
                              when label = 'grassland_p' then 2
                              when label = 'forest_np' then 3
                              when label = 'forest_p' then 4
                              when label = 'wetland_np' then 5
                              when label= 'wetland_p' then 6
                              when label = 'shrubland_np' then 7
                              when label = 'shrubland_p' then 8
                              END hex_order
                              FROM 
                              intact_clu.intactland_15_refined_cdl15_broad_pad
                              
                              ORDER BY value
                        ")



## Expansion:attach df to specific object in json #####################################################
states = get_postgis_query(con, "SELECT * FROM spatial.states WHERE st_abbrev IN ('IA','MN','ND','SD','NE','WY','MT')",
                                                geom_name = "geom")


setwd('D:\\intactland\\intact_clu\\final\\tiffs')
# r = raster('combined_region_groups_cdl15_broad_pad_rc_bs300m_mode_agg_median_3km_oops2.tif')
r = raster('combined_region_groups_cdl15_broad_pad_rc_bs300m_mode_agg_median_oops2.tif')

### convert the raster to SPDF
r_spdf <- as(r, "SpatialPixelsDataFrame")

###make the SPDF to a regular dataframe
r_df <- as.data.frame(r_spdf)


###remove all records with zero in it
row_sub = apply(r_df, 1, function(row) all(row !=0 ))
r_df <- r_df[row_sub,]

####add columns to the dataframe
colnames(r_df) <- c("value", "x", "y")

###join r_df df with hex_colors column
r_df<-merge(x=hex_colors,y=r_df,by="value")


####create new fill column with cut values
r_df$fill = cut(r_df$value, breaks= c(0,1,2,3,4,5,6,7,8,9,10,11,12))

############################################################################
#### get stats and create graphics ############################################
########################################################################

#### stats###########################################################
## get the counts per bin
table(r_df$hex)
print(hex_colors$hex)
#### graphics
d <- ggplot() + 
  ### state boundary background ###########
  geom_polygon(
  data=states, 
  aes(x=long,y=lat,group=group),
  fill='#BEBEBE') +


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
  size=1
  ) +

  
  # Equal scale cartesian coordinates 
  coord_equal() +
  
  #### add title to map #######
# labs(title = 'map title') +
  
  
  
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
    # legend.position = c(0.5, -0.01),
    legend.position="none",
    text = element_text(color = "#4e4d47", size=30)  ##these are the legend numeric values
  ) + 
  
  

  ##this is modifying the specifics of INSIDE the legend (i.e. the legends components that make up the legend)
  ### create a discrete scale. These functions allow you to specify your own set of mappings from levels in the data to aesthetic values.
  # scale_fill_manual(values = c("#023c9f","#a9d91a","#9c140a","#6c9e77","#023c9f","#a9d91a","#9c140a","#6c9e77","#023c9f","#a9d91a","#9c140a","#023c9f"),
  scale_fill_manual(values=hex_colors$hex, 
# scale_fill_manual(values = c(r_df$hex),
                    # scale_fill_manual(values = custom_pallete,
                    ###legend labels
                    # labels = c('<-7.5','-7.5','-5.0','-2.5','2.5','5.0','7.5','>7.5'),

                    #Legend type guide shows key (i.e., geoms) mapped onto values.
                    guide = guide_legend( title='',
                                          title.theme = element_text(
                                            size = 32,
                                            color = "#4e4d47",
                                            vjust=0.0,
                                            angle = 0
                                          ),
                                          # legend bin dimensions
                                          keyheight = unit(3, units = "mm"),
                                          keywidth = unit(20, units = "mm"),

                                          #legend elements position
                                          label.position = "bottom",
                                          title.position = 'top',

                                          #The desired number of rows of legends.
                                          nrow=1

                    )
  )
  
  
d

  # coord_equal() +
  # theme_map() +
  # theme(legend.position="bottom") +
  # theme(legend.key.width=unit(2, "cm"))



fileout = 'D:\\intactland\\graphics\\map_main.png'
ggsave(fileout, width = 11, height = 6.5, dpi = 500)
