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
# bb = get_postgis_query(con_synthesis, "SELECT ST_Intersection(states.geom, bb.geom) as geom
#                        FROM (SELECT st_transform(ST_MakeEnvelope(-111.144047, 36.585669, -79.748903, 48.760751, 4326),5070)as geom) as bb, spatial.states
#                        WHERE ST_Intersects(states.geom, bb.geom) ",
#                            geom_name = "geom")
#
# bb.df <- fortify(bb)


### Expansion:attach df to specific object in json #####################################################
states = get_postgis_query(con_synthesis, "SELECT geom FROM spatial.states WHERE st_abbrev
                                           IN ('MT','MN','IA','ND','SD')",
                                                geom_name = "geom")
summary(states)
states.df <- fortify(states)
summary(states.df)

## Expansion:attach df to specific object in json #####################################################
# region = get_postgis_query(con_synthesis, "SELECT wkb_geometry as geom FROM waterfowl.tstorm_dissolved_5070",
#                            geom_name = "geom")
#
# region.df <- fortify(region)



# ### Expansion:attach df to specific object in json #####################################################
region = get_postgis_query(con_synthesis, "SELECT geom FROM waterfowl.waterfowl_wgs84",
                           geom_name = "geom")

#### reproct to 5070 so aligned with other datasets
region <- spTransform(region, CRS("+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23 +lon_0=-96 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs
+ellps=GRS80 +towgs84=0,0,0"))

summary(region)

region.df <- fortify(region)
summary(region.df)
#
### Expansion:attach df to specific object in json #####################################################
states_large = get_postgis_query(con_synthesis, "SELECT geom FROM spatial.states",
                           geom_name = "geom")

states_large.df <- fortify(states_large)


### Expansion:attach df to specific object in json #####################################################
states = get_postgis_query(con_synthesis, "SELECT geom FROM spatial.states WHERE st_abbrev
                                           IN ('MT','IA','MN','ND','SD')",
                           geom_name = "geom")

states_region.df <- fortify(states)





fgdb = 'I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\habitat_impacts\\waterfowl\\data\\waterfowl.gdb'
mapa <- readOGR(dsn=fgdb,layer="s35_waterfowl_bs_maj900m_fc_dissolve")

# crs_wgs84 = CRS('+init=EPSG:4326')
# mapa <- spTransform(mapa, crs_wgs84)

#fortify() creates zany attributes so need to reattach the values from intial dataframe
mapa.df <- fortify(mapa)

#creates a numeric index for each row in dataframe
mapa@data$id <- rownames(mapa@data)

#merge the attributes of mapa@data to the fortified dataframe with id column
mapa.df <- join(mapa.df, mapa@data, by="id")
hist(mapa.df$gridcode,breaks = 50)
####this converts a Continuous value supplied to discrete scale!!!!!!!!!!!!
mapa.df$gridcode <- as.factor(mapa.df$gridcode)




##########################################################################
#### graphics############################################################
##########################################################################


d <- ggplot() + 
  ### state boundary background ###########
### states_large boundary background ###########
geom_polygon(
  data=states_large.df,
  aes(x=long,y=lat,group=group),
  fill='#f0f0f0') +
#   
  ### states_large boundary strokes ###########
geom_polygon(
  data=states_large.df,
  aes(y=lat, x=long, group=group),
  alpha=0,
  colour='white',
  size=6
) +

  ### states_region boundary background ###########
geom_polygon(
  data=states_region.df,
  aes(x=long,y=lat,group=group),
  fill='#cccccc') +
  
  
  ### states_region boundary strokes ###########
geom_polygon(
  data=states_region.df,
  aes(y=lat, x=long, group=group),
  alpha=0,
  colour='white',
  size=2
) +
  
  ### region boundary background ###########
geom_polygon(
  data=region.df,
  aes(x=long,y=lat,group=group),
  fill='#7e7e7e') + 
  
  
  
  
  geom_polygon(
    data=mapa.df,
    aes(x=long,y=lat, group=group, fill=gridcode)
    ) +
  
  ### state boundary strokes ###########
geom_polygon(
  data=states_region.df,
  aes(y=lat, x=long, group=group),
  alpha=0,
  colour='white',
  size=2
) + 
  

# coord_equal() + 
###### Equal scale cartesian coordinates 
######have to use projection limits and NOT lat long
coord_equal(xlim = c(-1250000,250000), ylim = c(2080000,3000000)) + 


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
    # plot.title = element_text(size= 45, vjust=-12.0, hjust=0.10, color = "#4e4d47"),
    
    ##shifts the entire legend (graphic AND labels)
    legend.justification = c(0,0),
    legend.position = c(0.30, 0.04),   ####(horizontal, vertical)
    legend.spacing.x = unit(0.5, 'cm'),
    text = element_blank()  ##these are the legend numeric values
    
  ) +

  ###this is modifying the specifics of INSIDE the legend (i.e. the legends components that make up the legend)
  ### create a discrete scale. These functions allow you to specify your own set of mappings from levels in the data to aesthetic values.
  scale_fill_manual(values = c("#0b2c7a", "#1e9094", "#0ec441", "#7bed00","#f7d707", "#e68e1c", "#c2523c"),
                    ###legend labels
                    labels = c('0-10','10-20','20-40','40-60','60-80','80-100','>100'),
                    #Legend type guide shows key (i.e., geoms) mapped onto values.
                    guide = guide_legend( title='',
                                          title.theme = element_text(
                                            size = 0,
                                            color = "#4e4d47",
                                            vjust=0.0,
                                            angle = 0,
                                            face="bold"
                                          ),
                                          # legend bin dimensions
                                          keyheight = unit(0.015, units = "npc"),
                                          keywidth = unit(0.05, units = "npc"),
                                          
                                          #legend elements position
                                          label.position = "bottom",
                                          title.position = 'top',
                                          
                                          #The desired number of rows of legends.
                                          nrow=1
                                          # byrow=TRUE
                                          
                    )
  )




getggplotObject <- function(x, multiplier, slots, labels){
  
  ###declare the empty list that will hold all the ggplot objects
  ggplot_object_list <- list()
  
  print('slots')
  print(slots)
  limit = x + (multiplier * slots)
  print('limit')
  print(limit)
  
  i = 1
  # labels <- c("20%","40%","60%","80%",">80%")
  while (x <= limit) {
    print('x-top')
    print(x)
    ggplot_object = annotation_custom(grobTree(textGrob(labels[i], x=x, y= 0.03, rot = 0,gp=gpar(col="#4e4d47", fontsize=45, fontface="bold"))))
    ggplot_object_list <- append(ggplot_object_list, list(ggplot_object))
    x = x + multiplier
    print('x-bottom')
    print(x)
    i = i + 1
  }
  return(ggplot_object_list)
  
}


legend_title_abandon = annotation_custom(grobTree(textGrob("Nesting Opportunities (pair/sq.mi.)", x=0.49, y= 0.09, rot = 0,gp=gpar(col="#4e4d47", fontsize=45, fontface="bold"))))
legendlabels_abandon <- getggplotObject(x = 0.30, multiplier = 0.056, slots = 7, labels = c("0","10","20","40","60","80","100"))

d + legend_title_abandon + legendlabels_abandon

fileout = 'I:\\d_drive\\projects\\usxp\\series\\s35\\deliverables\\habitat_impacts\\waterfowl\\deliverables\\test.png'
ggsave(fileout, width = 34, height = 25, dpi = 500)
