library(ggplot2)
library(maps)
library(rgdal)# R wrapper around GDAL/OGR
library(sp)
library(plyr)
library(dplyr)
library(viridis)
library(scales)
require(RColorBrewer)
library(glue)
library(ggpubr)
library(cowplot)
library(RPostgreSQL)
library(postGIStools)



root = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\temp\\R_ggplot2_map_demo_2019_06_07\\R_ggplot2_map_demo_2019_06_07\\'




con <- dbConnect(PostgreSQL(), dbname = 'nass', user = "mbougie",
                 host = '144.92.235.105',
                 port='5432',
                 password = 'Mend0ta!')

mapa <- get_postgis_query(con, "SELECT atlas_stco, perc_conv_county, st_transform(geom,4152) as geom FROM ag_census.ag_census_expansion",
                               geom_name = "geom")



# conn = psycopg2.connect("dbname='lem' user='mbougie' host='144.92.235.105' password='Mend0ta!'")

# require(sf)
# dsn = "PG:dbname='nass' host='144.92.235.105' port='5432' user='mbougie' password='Mend0ta!'"
# mapa <- st_read(dsn, "ag_census.ag_census_expansion")


  
########################################################################
##### create and modify dataframes #####################################
########################################################################

### create main dataframe (mapa.df)######################################

##spatial dataframe 
# mapa <- readOGR(dsn = "C:\\Users\\Bougie\\Box\\data_science_study_group\\resources\\R_ggplot2_map_demo_2019_06_07\\ggplot2_example_features.gdb",layer="agroibis_counties_example")
# mapa <- spTransform(mapa, CRS("+init=epsg:5070"))

##This function turns a map into a dataframe that can more easily be plotted with ggplot2.
mapa.df <- fortify(mapa)

#fortify() creates zany attributes so need to reattach the values from intial dataframe
#creates a numeric index for each row in dataframe
mapa@data$id <- rownames(mapa@data)

#merge the attributes of mapa@data to the fortified dataframe with id column
mapa.df <- join(mapa.df, mapa@data, by="id")

##need to do this step sometimes for some dataframes or geometry looks "torn"
# mapa.df <- mapa.df[order(mapa.df$order),]

##Use cut() function to divides a numeric vector into different ranges
##note: each bin must: 1)contain a value and 2)no records in dataframe can be null
mapa.df$fill = cut(mapa.df$perc_conv_county, breaks= c(0, 0.5, 2.5, 5, 7.5, 100))





#### bring in state shapefile for context in map ##################################
state.df <- readOGR(dsn = "C:\\Users\\Bougie\\Desktop\\Gibbs\\data\\usxp\\ancillary\\vector\\sf", layer = "states_wgs84")




########################################################################
##### create graphic ###################################################
########################################################################

# There are 3 essential elements to any ggplot call:
#   
# 1)An aesthetic that tells ggplot which variables are being mapped to the x axis, y axis, (and often other attributes of the graph, such as the color fill). Intuitively, the aesthetic can be thought of as what you are graphing.
# 2)A geom or geometry that tells ggplot about the basic structure of the graph. Intuitively, the geom can be thought of as how you are graphing it.
# 3)Other options, such as a graph title, axis labels and overall theme for the graph.



###ggplot() initializes a ggplot object##############
###It can be used to declare the input data frame for a graphic and to specify the set of plot 
###aesthetics intended to be common throughout all subsequent layers unless specifically overridden.
d = ggplot() + 
  


###A geom tells ggplot about the basic structure of the graph. Intuitively, the geom can be thought of as how you are graphing it.
###note: arrangement of geom_polygons important to how they are rendered on map

  ### state grey background ###########
   geom_polygon(
    data=state.df,
    aes(y=lat, x=long, group=group),
    fill='#cccccc'
  ) +
  
  
  ### county choropleth map ###########
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
   

  
  ### state boundary strokes ###########
  geom_polygon(
    data=state.df,
    aes(y=lat, x=long, group=group),
    alpha=0,
    colour='white',
    size=0.5
  ) + 
  
  #### define projection of ggplot object #######
  #### did not reproject the actual data just defined the projection of the map
  coord_map(project="polyconic") + 
  
  #### add title to map #######
  labs(title = "Phosphorus run-off from cropland expansion") +
  


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
    text = element_text(color = "#4e4d47", size=30),   ##these are the legend numeric values
    plot.title = element_text(size= 25, vjust=-12.0, hjust=0.20, color = "#4e4d47"),
    plot.caption = element_text(size= 18, color = "blue"),
    legend.position = c(0.12, -0.01)
  ) +



  ### create a discrete scale. These functions allow you to specify your own set of mappings from levels in the data to aesthetic values.
  scale_fill_manual(values = rev(brewer.pal(5, 'PRGn')[1:5]), ##reference colorbrewer list of hex values

                    ###legend labels
                    labels = c("0.5", "2.5", "5", "7.5", ">7.5"),

                    #Legend type guide shows key (i.e., geoms) mapped onto values.
                    guide = guide_legend( title='kg per county',
                                          title.theme = element_text(
                                            size = 27,
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



# ###create panel image ######################
dir = "D:\\projects\\usxp\\series\\s35\\maps\\choropleths\\maps\\png\\"
fileout=paste(dir,"ggplot2_map_example2",".png", sep="")
ggsave(fileout, width = 20, height = 20, dpi = 800)



