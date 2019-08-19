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
# # ### Expansion:attach df to specific object in json #####################################################
# # bb = get_postgis_query(con_synthesis, "SELECT ST_Intersection(states.geom, bb.geom) as geom 
# #                        FROM (SELECT st_transform(ST_MakeEnvelope(-111.144047, 36.585669, -79.748903, 48.760751, 4326),5070)as geom) as bb, spatial.states 
# #                        WHERE ST_Intersects(states.geom, bb.geom) ",
# #                            geom_name = "geom")
# # 
# # bb.df <- fortify(bb)
# fgdb = 'I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\habitat_impacts\\milkweed\\data\\milkweed.gdb'
# mapa <- readOGR(dsn=fgdb,layer="milkweed_bs3km_region")
# 
# crs_wgs84 = CRS('+init=EPSG:4326')
# mapa <- spTransform(mapa, crs_wgs84)
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
# ### Expansion:attach df to specific object in json #####################################################
# states = get_postgis_query(con_synthesis, "SELECT st_transform(geom,4326) as geom FROM spatial.states WHERE st_abbrev
#                                            IN ('IL','IN','IA','KS','KY','MI','MN','MO','NE','ND','OH','SD','WI')",
#                                                 geom_name = "geom")
# 
# states.df <- fortify(states)
# 
# ### Expansion:attach df to specific object in json #####################################################
# states_large = get_postgis_query(con_synthesis, "SELECT st_transform(geom,4326) as geom FROM spatial.states",
#                            geom_name = "geom")
# 
# states_large.df <- fortify(states_large)

# setwd('I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\habitat_impacts\\milkweed\\data\\tiffs')
# r = raster('milkweed_bs3km_region.tif')
# 
# # ###repoject to wgs84
# # crs_wgs84 = CRS('+init=EPSG:4326')
# # r <- projectRaster(r, crs=crs_wgs84)
# 
# ### convert the raster to SPDF
# r_spdf <- as(r, "SpatialPixelsDataFrame")
# 
# 
# 
# ###make the SPDF to a regular dataframe
# r_df <- as.data.frame(r_spdf)
# 
# ###remove all records with zero in it
# row_sub = apply(r_df, 1, function(row) all(row !=0 ))
# r_df <- r_df[row_sub,]
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
# r_df$fill = cut(r_df$value_round, breaks= c(3, 11105, 36153, 76964, 150124, 485515))
# 
# 
# 
# # new df retaining only rows where value_round does not equal 0
# ##indexing rows with a cond
# ##### r_df_new = r_df[r_df$value_round != 0 |&,]
# 
# 
# #### stats
# ## get the counts per bin
# table(r_df$fill)





#### graphics



ggplot() + 
  ### state boundary background ###########
### state boundary background ###########
geom_polygon(
  data=states_large.df,
  aes(x=long,y=lat,group=group),
  # fill='#D0D0D0') +
  fill='#D3D3D3') +
  
  ### state boundary strokes ###########
geom_polygon(
  data=states_large.df,
  aes(y=lat, x=long, group=group),
  alpha=0,
  colour='white',
  size=1
) +
  ### state boundary background ###########
  geom_polygon(
  data=states.df,
  aes(x=long,y=lat,group=group),
  # fill='#D0D0D0') +
  fill='#808080') +

geom_polygon(
  data=mapa.df,
  aes(x=long,y=lat,group=group),
  # fill='#D0D0D0') +
  fill=mapa.df$gridcode) +

  ### state boundary strokes ###########
geom_polygon(
  data=states.df,
  aes(y=lat, x=long, group=group),
  alpha=0,
  colour='white',
  size=1
) +


  # Equal scale cartesian coordinates 
# coord_equal() 
#   
# coord_map(project="polyconic")
  
coord_map(project="polyconic", xlim = c(-105,-77),ylim = c(35.5, 50)) + 

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
    legend.position = c(0.2, -0.01),
    text = element_text(color = "#4e4d47", size=30)  ##these are the legend numeric values
  ) +


  ###this is modifying the specifics of INSIDE the legend (i.e. the legends components that make up the legend)
  ### create a discrete scale. These functions allow you to specify your own set of mappings from levels in the data to aesthetic values.
  scale_fill_manual(values = c('#00441b','#1b7837','#5aae61','#a6dba0','#f4a582','#d6604d','#b2182b','#67001f'),
                    # scale_fill_manual(values = custom_pallete,
                    ###legend labels
                    labels = c('<-7.5','-7.5','-5.0','-2.5','2.5','5.0','7.5','>7.5'),

                    #Legend type guide shows key (i.e., geoms) mapped onto values.
                    guide = guide_legend( title='legend title',
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
  
  
  

  # coord_equal() +
  # theme_map() +
  # theme(legend.position="bottom") +
  # theme(legend.key.width=unit(2, "cm"))



fileout = 'I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\gross_net\\images\\s35_bs3km_net_png500_r_no_alpha2.png'
ggsave(fileout, width = 34, height = 38, dpi = 500)
