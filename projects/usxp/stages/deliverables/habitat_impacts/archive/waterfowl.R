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
bb = get_postgis_query(con_synthesis, "SELECT ST_Intersection(states.geom, bb.geom) as geom
                       FROM (SELECT st_transform(ST_MakeEnvelope(-111.144047, 36.585669, -79.748903, 48.760751, 4326),5070)as geom) as bb, spatial.states
                       WHERE ST_Intersects(states.geom, bb.geom) ",
                           geom_name = "geom")

bb.df <- fortify(bb)


### Expansion:attach df to specific object in json #####################################################
states = get_postgis_query(con_synthesis, "SELECT st_transform(geom,4326) as geom FROM spatial.states WHERE st_abbrev
                                           IN ('MT','MN','IA','ND','SD')",
                                                geom_name = "geom")

states.df <- fortify(states)


## Expansion:attach df to specific object in json #####################################################
region = get_postgis_query(con_synthesis, "SELECT st_transform(wkb_geometry,4326) as geom FROM waterfowl.tstorm_dissolved_5070",
                           geom_name = "geom")

region.df <- fortify(region)



### Expansion:attach df to specific object in json #####################################################
region = get_postgis_query(con_synthesis, "SELECT geom FROM waterfowl.waterfowl_wgs84",
                           geom_name = "geom")

region.df <- fortify(region)

# 
### Expansion:attach df to specific object in json #####################################################
states_large = get_postgis_query(con_synthesis, "SELECT st_transform(geom,4326) as geom FROM spatial.states",
                           geom_name = "geom")

states_large.df <- fortify(states_large)


### Expansion:attach df to specific object in json #####################################################
states = get_postgis_query(con_synthesis, "SELECT st_transform(geom,4326) as geom FROM spatial.states WHERE st_abbrev
                                           IN ('MT','IA','MN','ND','SD')",
                           geom_name = "geom")

states_region.df <- fortify(states)





fgdb = 'I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\habitat_impacts\\waterfowl\\data\\waterfowl.gdb'
mapa <- readOGR(dsn=fgdb,layer="s35_waterfowl_bs3km")

crs_wgs84 = CRS('+init=EPSG:4326')
mapa <- spTransform(mapa, crs_wgs84)

#fortify() creates zany attributes so need to reattach the values from intial dataframe
mapa.df <- fortify(mapa)

#creates a numeric index for each row in dataframe
mapa@data$id <- rownames(mapa@data)

#merge the attributes of mapa@data to the fortified dataframe with id column
mapa.df <- join(mapa.df, mapa@data, by="id")





##########################################################################
#### graphics############################################################
##########################################################################


ggplot() + 
  ### state boundary background ###########
### state boundary background ###########
geom_polygon(
  data=states_large.df,
  aes(x=long,y=lat,group=group),
  fill='#f0f0f0') +
#   
  ### state boundary strokes ###########
geom_polygon(
  data=states_large.df,
  aes(y=lat, x=long, group=group),
  alpha=0,
  colour='white',
  size=6
) +

  ### state boundary background ###########
geom_polygon(
  data=states_region.df,
  aes(x=long,y=lat,group=group),
  fill='#cccccc') +
  
  
  ### state boundary strokes ###########
geom_polygon(
  data=states_region.df,
  aes(y=lat, x=long, group=group),
  alpha=0,
  colour='white',
  size=2
) +
  
  ### state boundary background ###########
geom_polygon(
  data=region.df,
  aes(x=long,y=lat,group=group),
  fill='#7e7e7e') + 
  
  
  
  
  geom_polygon(
    data=mapa.df,
    aes(x=long,y=lat,group=group),
    fill=mapa.df$gridcode) +
  
  
  


  # Equal scale cartesian coordinates 
# coord_equal() 
#   
# coord_map(project="polyconic")
coord_map(project="polyconic", xlim = c(-114,-90),ylim = c(39, 50)) +   



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
  scale_fill_manual(values = c('#00441b','#1b7837','#5aae61','#a6dba0','#f4a582','#d6604d','#b2182b'),
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




fileout = 'I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\habitat_impacts\\waterfowl\\deliverables\\test.png'
ggsave(fileout, width = 34, height = 25, dpi = 500)
