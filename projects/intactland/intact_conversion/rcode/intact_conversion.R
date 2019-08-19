# 
# library(ggplot2)
# library(maps)
# library(rgdal)# R wrapper around GDAL/OGR
# library(sp)
# require("RPostgreSQL")
# library(plyr)
# library(dplyr)
# library(viridis)
# library(scales)
# drv <- dbDriver("PostgreSQL")
# 
# con <- dbConnect(drv, dbname = "intactlands",
#                  host = "144.92.235.105", port = 5432,
#                  user = "mbougie", password = "Mend0ta!")
# 
# 
# 
# ##########################  using supplied counties dataset######################################   
# 
# #### get the built in state feature layer
# state <- map_data("state") 
# 
# #### subset state dataset
# # state_ss <- subset(state, region=='montana' | region=='wyoming' | region == 'north dakota'| region=='south dakota' | region == 'minnesota' | region=='iowa' | region == 'nebraska')
# state_ss <- subset(state, region == 'north dakota')
# 
# #### create counties dataset from buit in dataset
# cnty_intial <- map_data("counties")
# 
# #### get the fips code
# data(counties.fips)
# 
# #### attach fips code to counties dataset
# cnty <- cnty_intial %>%
#   mutate(polyname = paste(region,subregion,sep=",")) %>%
#   left_join(counties.fips, by="polyname")
# 
# ## query the data from postgreSQL 
# postgres_query <- dbGetQuery(con, "SELECT atlas_stco, percent FROM counts.abandoned_nd;")
# 
# ## merge cnty2 with postgres_query
# d = merge(cnty, postgres_query, sort = TRUE, by.x='fips', by.y='atlas_stco')
# 
# ## If I don't reorder the order of lat long it "tears" the polygons!
# cnty_fnl<-d[order(d$order),]
# 
# 
# d = ggplot() + 
#   geom_polygon(
#     data=cnty_fnl,
#     aes(y=lat, x=long, group=group, fill = percent),
#     colour = 'grey50',
#     size = 0.25
#   ) + 
#   scale_fill_distiller(name="Percent", palette = "Greens", breaks = pretty_breaks(n = 5), trans = "reverse")+
#   # scale_fill_continuous(name="percent", trans = "reverse") +
#   geom_polygon(
#     data=state,
#     aes(y=lat, x=long, group=group),
#     fill='grey70',
#     alpha=0,
#     colour='white',
#     size=0.9
#   )+
#   coord_map(project="polyconic") +
#   labs(
#     title = "Percent Abandoned by counties",
#     subtitle = "For cumulative year 2015"
#   ) +
#   theme(
#     text = element_text(color = "#22211d"), 
#     axis.text.x = element_blank(),
#     axis.title.x=element_blank(),
#     axis.text.y = element_blank(),
#     axis.title.y=element_blank(),
#     axis.ticks = element_blank(),
#     panel.grid.major = element_blank(),
#     plot.background = element_rect(fill = "white", color = NA), 
#     panel.background = element_rect(fill = "white", color = NA), 
#     legend.background = element_rect(fill = "white", color = NA),
#     plot.title = element_text(size= 15, hjust=0.01, color = "#4e4d47", margin = margin(b = -0.1, t = 0.1, l = 0.01, unit = "cm")),
#     plot.subtitle = element_text(size= 10, hjust=0.01, color = "#4e4d47", margin = margin(b = -0.1, t = 0.20, l = 2, unit = "cm")),
#     legend.position = c(0.09, -0.01)
#   )
# 
# d + guides(fill = guide_colorbar(reverse = TRUE, barwidth = 7, barheight = 0.5, title.position = 'top')) + theme(legend.direction = "horizontal")
# 


#################################################################################################
################################new ###########################################
#############################################################################################################



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

# rootpath = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\synthesis\\extensification'
# 
# #####link to the other two scripts
# source(paste(rootpath, 'rcode\\extensification_maps.R', sep='\\'))
# 
# json_file = paste(rootpath, 'json\\json_panels.json', sep='\\')
# jsondata <- fromJSON(file=json_file)
# 


user <- "mbougie"
host <- '144.92.235.105'
port <- '5432'
password <- 'Mend0ta!'

### Make the connection to database ######################################################################
con <- dbConnect(PostgreSQL(), dbname = 'intactland', user = user, host = host, port=port, password = password)



query1 = "SELECT 
          a.label, 
          a.atlas_stco, 
          a.acres,
          c.acres_calc,
          ROUND(((a.acres/c.acres_calc)*100)::numeric,0) as perc,
          ST_Transform(counties_102003.wkb_geometry,4326) as geom
          FROM 
          intact_conversion.intact_conversion_11and15_hist_counties as a INNER JOIN 
          spatial.counties_102003 as c
          ON
          c.atlas_stco = a.atlas_stco
          WHERE label = '4' 
          AND intact_conversion_11and15_hist_counties.atlas_stco NOT IN ('56013', '27119', '27111') 
          AND ROUND(((intact_conversion_11and15_hist_counties.acres/counties_102003.acres_calc)*100)::numeric,0) > 0
          ORDER BY (a.acres/c.acres_calc)*100 desc
          "


query2 = "SELECT 
          a.label, 
          a.atlas_stco, 
          a.acres,
          -- b.acres,
          c.acres_calc,
          ROUND(((a.acres/b.acres)*100)::numeric,0) as perc,
          ST_Transform(c.wkb_geometry,4326) as geom
          FROM 
          intact_conversion.intact_conversion_11and15_hist_counties as a INNER JOIN 
          spatial.counties_102003 as c
          ON
          c.atlas_stco = a.atlas_stco
          INNER JOIN
          intact_conversion.intactland_11_refined_hist_counties as b
          ON 
          c.atlas_stco = b.atlas_stco
          WHERE a.label = '4' AND b.label='11' AND b.acres <> 0 AND ROUND(((a.acres/b.acres)*100)::numeric,0) > 0 
          ORDER BY (a.acres/c.acres_calc)*100 desc"


###########################################################################################
#####get the dataframes###################################################################
###########################################################################################

### Expansion:attach df to specific object in json #####################################################
mapa <- get_postgis_query(con, 
                        query2,
                        geom_name = "geom")



#### bring in state shapefile for context in map ##################################
# counties.df <- readOGR(dsn = "I:\\e_drive\\data\\usxp\\ancillary\\vector\\sf", layer = "states_wgs84")
### Expansion:attach df to specific object in json #####################################################
counties.df <- get_postgis_query(con, 
                                "SELECT 
                                ST_Transform(counties_102003.wkb_geometry,4326) as geom
                                FROM 
                                spatial.counties_102003
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


###########################################################################################
#####visualize dataframe###################################################################
###########################################################################################
d = ggplot() +

  ### state grey background ###########
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
  aes(y=lat, x=long, group=group, fill = perc),
  colour = '#cccccc',
  size = 0.5
) +
  
### state boundary strokes ###########
geom_polygon(
  data=counties.df,
  aes(y=lat, x=long, group=group),
  alpha=0,
  colour='white',
  size=0.5
) + 
  
#### define projection of ggplot object #######
#### did not reproject the actual data just defined the projection of the map
coord_map(project="polyconic") +

#### add title to map #######
labs(title = 'percent loss relative to counties acreage for clu_intact_11 to clu_intact_15')

d





# ################ map ################################################
# 
# 
# 
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
# createMap <- function(obj){
#   print('obj$column')
#   print(obj$column)
#   ### get the spatial dtaframe that is stored in the object
#   mapa <- obj$df
#   
#   ########################################################################
#   ##### modify dataframes #####################################
#   ########################################################################
#   
#   #fortify() creates zany attributes so need to reattach the values from intial dataframe
#   mapa.df <- fortify(mapa)
#   
#   #creates a numeric index for each row in dataframe
#   mapa@data$id <- rownames(mapa@data)
#   
#   #merge the attributes of mapa@data to the fortified dataframe with id column
#   mapa.df <- join(mapa.df, mapa@data, by="id")
#   
#   ##need to do this step sometimes for some dataframes or geometry looks "torn"
#   # mapa.df <- mapa.df[order(mapa.df$order),]
#   
#   
#   
#   #Use cut() function to divides a numeric vector into different ranges
#   #note: each bin must: 1)contain a value and 2)NO records in dataframe can be null
#   ###function to fill certain columns values
#   getFillValues <- function(){
#     # print(obj$column)
#     v <- c('p_aban_imp_rfs', 'n_aban_imp_rfs', 'sed_aban_imp_rfs', 'et_aban_imp_rfs_gal', 'ratio_abandon_rfs_mlra')
#     # print(match(obj$column,v))
#     if(obj$column %in% v){
#       print('--------match-----------------')
#       mapa.df$current_field <- mapa.df$current_field * -1
#       # print(mapa.df$current_field)
#       return(cut(mapa.df$current_field, breaks= obj$bin_breaks))
#     }
#     else{
#       print('-------NO-match-----------------')
#       # print(cut(mapa.df$current_field, breaks= obj$bin_breaks))
#       return(cut(mapa.df$current_field, breaks= obj$bin_breaks))
#     }
#   }
#   
#   ####create new fill column with cut values returned from getFillValues() function
#   mapa.df$fill = getFillValues()
#   
#   
#   #### bring in state shapefile for context in map ##################################
#   state.df <- readOGR(dsn = "I:\\e_drive\\data\\usxp\\ancillary\\vector\\sf", layer = "states_wgs84")
#   
#   
#   
#   
#   ########################################################################
#   ##### create graphic ###################################################
#   ########################################################################
#   
#   # There are 3 essential elements to any ggplot call:
#   #
#   # 1)An aesthetic that tells ggplot which variables are being mapped to the x axis, y axis, (and often other attributes of the graph, such as the color fill). Intuitively, the aesthetic can be thought of as what you are graphing.
#   # 2)A geom or geometry that tells ggplot about the basic structure of the graph. Intuitively, the geom can be thought of as how you are graphing it.
#   # 3)Other options, such as a graph title, axis labels and overall theme for the graph.
#   
#   
#   
#   ###ggplot() initializes a ggplot object##############
#   ###It can be used to declare the input data frame for a graphic and to specify the set of plot
#   ###aesthetics intended to be common throughout all subsequent layers unless specifically overridden.
#   d = ggplot() +
#     
#     
#     
#     ###A geom tells ggplot about the basic structure of the graph. Intuitively, the geom can be thought of as how you are graphing it.
#     ###note: arrangement of geom_polygons important to how they are rendered on map
#     
#     ### state grey background ###########
#   geom_polygon(
#     data=state.df,
#     aes(y=lat, x=long, group=group),
#     fill='#cccccc'
#   ) +
#     
#     
#     ### counties choropleth map ###########
#   geom_polygon(
#     data=mapa.df,
#     ###Aesthetic tells ggplot which variables are being mapped to the x axis, y axis,
#     ###(and often other attributes of the graph, such as the color fill).
#     ###Intuitively, the aesthetic can be thought of as what you are graphing.
#     
#     ###y-axis of graph referencing lat column
#     ###x-axis of graph referencing long column
#     ###group tells ggplot that the data has explicit groups
#     ###fill color of features referencing fill column. Fill color is initially arbitrary (changing the color of fill will be addressed later in code)
#     aes(y=lat, x=long, group=group, fill = fill),
#     colour = '#cccccc',
#     size = 0.5
#   ) +
#     
#     
#     
#     ### state boundary strokes ###########
#   geom_polygon(
#     data=state.df,
#     aes(y=lat, x=long, group=group),
#     alpha=0,
#     colour='white',
#     size=0.5
#   ) +
#     
#     #### define projection of ggplot object #######
#   #### did not reproject the actual data just defined the projection of the map
#   coord_map(project="polyconic") +
#     
#     #### add title to map #######
#   labs(title = obj$title) +
#     
#     
#     
#     theme(
#       #### nulled attributes ##################
#       axis.text.x = element_blank(),
#       axis.title.x=element_blank(),
#       axis.text.y = element_blank(),
#       axis.title.y=element_blank(),
#       axis.ticks = element_blank(),
#       axis.line = element_blank(),
#       
#       panel.background = element_rect(fill = NA, color = NA),
#       panel.grid.major = element_blank(),
#       
#       plot.background = element_rect(fill = NA, color = NA),
#       plot.margin = unit(c(0, 0, 0, 0), "cm"),
#       
#       #### modified attributes ########################
#       text = element_text(color = "#4e4d47", size=30),   ##these are the legend numeric values
#       plot.title = element_text(size= 35, vjust=-12.0, hjust=0.20, color = "#4e4d47"),
#       plot.caption = element_text(size= 18, color = "blue"),
#       legend.position = c(0.12, -0.01)
#     ) +
#     
#     
#     
#     ### create a discrete scale. These functions allow you to specify your own set of mappings from levels in the data to aesthetic values.
#     scale_fill_manual(values = rev(brewer.pal(10, 'PRGn')[obj$legend_range]),
#                       # scale_fill_manual(values = custom_pallete,
#                       ###legend labels
#                       labels = obj$legend_labels,
#                       
#                       #Legend type guide shows key (i.e., geoms) mapped onto values.
#                       guide = guide_legend( title=obj$legend_title,
#                                             title.theme = element_text(
#                                               size = 32,
#                                               color = "#4e4d47",
#                                               vjust=0.0,
#                                               angle = 0
#                                             ),
#                                             # legend bin dimensions
#                                             keyheight = unit(3, units = "mm"),
#                                             keywidth = unit(20, units = "mm"),
#                                             
#                                             #legend elements position
#                                             label.position = "bottom",
#                                             title.position = 'top',
#                                             
#                                             #The desired number of rows of legends.
#                                             nrow=1
#                                             
#                       )
#     )
#   
#   return(d)
#   
# } 


