

######################## main #####################################################################
library(ggplot2)
library(maps)
library(rgdal)# R wrapper around GDAL/OGR
library(sp)
require("RPostgreSQL")
library(postGIStools)
library(plyr)
# library(dplyr)
library(viridis)
library(scales)
library(rjson)
# library(jsonlite)
require(RColorBrewer)
library(glue)
# library(ggpubr)
library(cowplot)




user <- "mbougie"
host <- '144.92.235.105'
port <- '5432'
password <- 'Mend0ta!'

### Make the connection to database ######################################################################
con <- dbConnect(PostgreSQL(), dbname = 'intactland', user = user, host = host, port=port, password = password)

## Expansion:attach df to specific object in json #####################################################
states = get_postgis_query(con, "SELECT * FROM spatial.states WHERE st_abbrev IN ('IA','MN','ND','SD','NE','WY','MT')",
                           geom_name = "geom")


###------------------------------------------------------
###--------this is the NCLD comparison dataset-----------
###------------------------------------------------------
query_compare_nlcd = "SELECT
                      ROUND(((overlap.acres/total.acres) * 100)::numeric,0)::integer as perc,
                      ST_Transform(counties.wkb_geometry,4326) as geom
                      FROM
                      ----this temp table selects the records where there is overlap between nlcd and clu dataset (i.e. 0/0 or 1/1) --- 4 records in all
                      (SELECT 
                      atlas_stco,
                      sum(acres) as acres
                      FROM 
                      intact_compare.intact_compare_region_refined_hist_counties 
                      WHERE label IN ('1','2','5','8')
                      GROUP BY atlas_stco
                      HAVING SUM(acres) <> 0) as overlap
                      
                      INNER JOIN 
                      
                      ----this temp table selects contains all rows 
                      (SELECT 
                      atlas_stco,
                      sum(acres) as acres
                      FROM 
                      intact_compare.intact_compare_region_refined_hist_counties 
                      GROUP BY atlas_stco
                      HAVING SUM(acres) <> 0) as total
                      
                      USING(atlas_stco)
                      
                      INNER JOIN
                      
                      spatial.counties_102003 as counties
                      
                      USING(atlas_stco)
                      
                      ---WHERE st_abbrev IN ('SD')   
                      "


###########################################################################################
#####get the dataframes###################################################################
###########################################################################################

### Expansion:attach df to specific object in json #####################################################
mapa <- get_postgis_query(con,
                        ### NLCD ##############
                        query_compare_nlcd,

                        geom_name = "geom")



#### bring in state shapefile for context in map ##################################
# counties.df <- readOGR(dsn = "I:\\e_drive\\data\\usxp\\ancillary\\vector\\sf", layer = "states_wgs84")
### Expansion:attach df to specific object in json #####################################################
counties.df <- get_postgis_query(con, 
                                "SELECT 
                                ST_Transform(counties_102003.wkb_geometry,4326) as geom
                                ----counties_102003.wkb_geometry as geom
                                FROM 
                                spatial.counties_102003

                                ----NLCD--------------
                                WHERE st_abbrev IN ('MN','IA','ND','SD','NE','MT','WY')
                 
                                ",
                                geom_name = "geom")


###########################################################################################
#####modify dataframe###################################################################
###########################################################################################


#fortify() creates zany attributes so need to reattach the values from intial dataframe
mapa.df <- fortify(mapa)

#creates a numeric index for each row in dataframe
mapa@data$id <- rownames(mapa@data)

#merge the attributes of mapa@data to the fortified dataframe with id column
mapa.df <- join(mapa.df, mapa@data, by="id")

####NLCD##########################
mapa.df$fill <- cut(mapa.df$perc,  breaks= c(65,70,75,80,85,90,95,100))


table(mapa.df$perc)
table(mapa.df$fill)
###########################################################################################
#####visualize dataframe###################################################################
###########################################################################################
d = ggplot() +

  ### county grey background ###########
geom_polygon(
  data=counties.df,
  aes(y=lat, x=long, group=group),
  fill='#cccccc'
) +  
  
  
### counties choropleth map ###########
geom_polygon(
  data=mapa.df,
  ###Aesthetic tells ggplot which variables are being mapped to the x axis, y axis,
  ###(and often other attributes of the graph, such as the color fill).
  ###Intuitively, the aesthetic can be thought of as what you are graphing.
  
  ###y-axis of graph referencing lat column
  ###x-axis of graph referencing long column
  ###group tells ggplot that the data has explicit groups
  ###fill color of features referencing fill column. Fill color is initially arbitrary (changing the color of fill will be addressed later in code)
  aes(y=lat, x=long, group=group, fill = fill),
  colour = '#cccccc',
  size = 0.5
) +
  
### county boundary strokes ###########
geom_polygon(
  data=counties.df,
  aes(y=lat, x=long, group=group),
  alpha=0,
  colour='white',
  size=0.25
) + 
  
#   ### state boundary strokes ###########
# geom_polygon(
#   data=states,
#   aes(y=lat, x=long, group=group),
#   # alpha=0,
#   fill=NA,
#   colour='white',
#   size=1
# ) +
  
#### define projection of ggplot object #######
# Equal scale cartesian coordinates 
# coord_equal() +
#### did not reproject the actual data just defined the projection of the map
coord_map(project="polyconic") +
  
# coord_map("albers", lat0=30, lat1=40) + 


#### add title to map #######
### NLCD ##############
labs(title = 'Intact compare NLCD',
     caption = '      70          75         80         85         90         95         100') + 



### create a discrete scale. These functions allow you to specify your own set of mappings from levels in the data to aesthetic values.
  # scale_fill_manual(values = brewer.pal(7, 'OrRd'),
  scale_fill_manual(values = c('#edf2f7','#dbe4f0', '#b0c4de', '#6f93c3', '#446ca2', '#2d486c','#172436'),

                  labels = c('65','70','75','80','85','90','95'),                 
                  
                  #Legend type guide shows key (i.e., geoms) mapped onto values.
                  guide = guide_legend( title='overlap/total',
                                        title.theme = element_text(
                                          size = 25,
                                          color = "#4e4d47",
                                          vjust=0.0,
                                          angle = 0
                                        ),
                                        # legend bin dimensions
                                        keyheight = unit(3, units = "mm"),
                                        keywidth = unit(15, units = "mm"),
                                        
                                        #legend elements position
                                        label.position = "bottom",
                                        title.position = 'top',
                                        
                                        #The desired number of rows of legends.
                                        nrow=1
                                        
                  )
  ) + 
  
  
  
  
  
  
  
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
    text = element_text(color = "#4e4d47", size=25),   ##these are the legend numeric values
    legend.text = element_text(color='white', size=0),
    legend.position = c(0.0, 0.07), #### legend position
    plot.title = element_text(size= 25, vjust=-5.0, hjust=0.5, color = "#4e4d47"), ###title size/position/color
    plot.caption = element_text(size= 15, vjust=9, hjust=0.05, color = "#4e4d47") ###title size/position/color
  ) 

### NLCD ##############
ggsave('D:\\intactland\\graphics\\compare\\compare_nlcd.png', width = 11, height=8.5, dpi = 500)




