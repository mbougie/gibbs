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
library(grid)
library(scales)
#library(viridis)  # better colors for everyone
library(ggthemes) # theme_map()


rootpath = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\synthesis\\extensification'

#####link to the other two scripts
source(paste(rootpath, 'rcode\\extensification_maps.R', sep='\\'))

json_file = paste(rootpath, 'schemas\\seth\\json\\json_panels_seth.json', sep='\\')

jsondata <- fromJSON(file=json_file)



user <- "mbougie"
host <- '144.92.235.105'
port <- '5432'
password <- 'Mend0ta!'

### Make the connection to database ######################################################################
con_synthesis <- dbConnect(PostgreSQL(), dbname = 'synthesis', user = user, host = host, port=port, password = password)





###########################################################################################
#####get the dataframes###################################################################
###########################################################################################


### Main query that all the datasets will reference #####################################################
query_ext <- 'SELECT
"dataset".atlas_stco,
"dataset".mean,
("dataset".mean * conversion_table.conv_factor)  as current_field,
\'lookup\' as dataset,
st_transform(geom,4326) as geom
FROM
extensification_seth."dataset"
INNER JOIN spatial.counties
ON "dataset".atlas_stco = counties.fips
INNER JOIN misc.conversion_table ON \'lookup\' = conversion_table.extensification'

query_ext_area <- 'SELECT
 lrr_group,
sum(mean),
ST_Area(ST_Union(ST_SnapToGrid(geom,0.0001))) * 0.00024710538146717 as acres,
(sum(mean)/(ST_Area(ST_Union(ST_SnapToGrid(geom,0.0001))) * 0.00024710538146717))*100 as current_field,
st_transform(ST_Union(ST_SnapToGrid(geom,0.0001)),4326) as geom
FROM 
extensification_seth.ext_acres_fips_summary as a INNER JOIN
spatial.counties  
ON
counties.fips = a.atlas_stco
INNER JOIN misc.conversion_table ON \'lookup\' = conversion_table.extensification
GROUP BY lrr_group'


### Main query that all the datasets will reference #####################################################
query_abd <- 'SELECT
"dataset".atlas_stco,
"dataset".mean,
("dataset".mean * conversion_table.conv_factor)*-1  as current_field,
\'lookup\' as dataset,
geom
FROM
extensification_seth."dataset"
INNER JOIN spatial.counties
ON "dataset".atlas_stco = counties.fips
INNER JOIN misc.conversion_table ON \'lookup\' = conversion_table.extensification'


query_abd_area <- 'SELECT
 lrr_group,
sum(mean),
ST_Area(ST_Union(ST_SnapToGrid(geom,0.0001))) * 0.00024710538146717 as acres,
((sum(mean)/(ST_Area(ST_Union(ST_SnapToGrid(geom,0.0001))) * 0.00024710538146717))*100)*-1 as current_field,
st_transform(ST_Union(ST_SnapToGrid(geom,0.0001)),4326) as geom
FROM 
extensification_seth.abd_acres_fips_summary as a INNER JOIN
spatial.counties  
ON
counties.fips = a.atlas_stco
INNER JOIN misc.conversion_table ON \'lookup\' = conversion_table.extensification
GROUP BY lrr_group'
###########################################################################################

rm(jsondata)
jsondata <- fromJSON(file=json_file)

obj = jsondata$agroibis$TPrunoff_abd
# print(obj)

# query_specific <- gsub("dataset",obj$dataset,query_ext_area)
# query_specific <- gsub("lookup",obj$lookup,query_specific)
# 
# print(query_specific)
# obj$df <- get_postgis_query(con_synthesis,query_specific,geom_name = "geom")


## abd #####
query_specific <- gsub("dataset",obj$dataset,query_abd)
query_specific <- gsub("lookup",obj$lookup,query_specific)

print(query_specific)
obj$df <- get_postgis_query(con_synthesis,query_specific,geom_name = "geom")










######################################################################
################ analysis of specific df ##############################
#######################################################################

### get the spatial dtaframe that is stored in the object
mapa <- obj$df


temp <- mapa@data
hist(temp$current_field, 100)
summary(temp$current_field)


# temp$current_field[temp$current_field<1] <- NA
# temp$current_field[temp$current_field>25] <- 500
hist(temp$current_field, 100)

print(obj$bin_breaks)
#### create legend labels ##################################

####remove zero from labels 
# labels = subset(obj$bin_breaks, !(obj$bin_breaks %in% c(0)))
# print(labels)
labels = obj$bin_breaks
labels[labels == 1e300] <- ""
labels[labels == -1e300] <- ""

print(labels)



temp$bins = cut(temp$current_field, breaks= obj$bin_breaks)
table(temp$bins)






########################################################################
##### modify dataframes #####################################
########################################################################

#fortify() creates zany attributes so need to reattach the values from intial dataframe
mapa.df <- fortify(mapa)

#creates a numeric index for each row in dataframe
mapa@data$id <- rownames(mapa@data)

#merge the attributes of mapa@data to the fortified dataframe with id column
mapa.df <- join(mapa.df, mapa@data, by="id")



hist(mapa.df$current_field, 100)




# getFillValues <- function(){
#   #### if the dataset is abandonment then flip the color scheme ####################
#   if(grepl('abd', obj$dataset)){
#     print('--------match-----------------')
#     mapa.df$current_field <- mapa.df$current_field * -1
# 
#     return(cut(mapa.df$current_field, breaks= obj$bin_breaks))
#   }
#   else{
#     print('-------NO-match-----------------')
#     # print(cut(mapa.df$current_field, breaks= obj$bin_breaks))
#     return(cut(mapa.df$current_field, breaks= obj$bin_breaks))
#   }
# }






###create new fill column with cut values returned from getFillValues() function
# mapa.df$fill = getFillValues()
mapa.df$fill = cut(mapa.df$current_field, breaks= obj$bin_breaks)
table(mapa.df$fill)

#### bring in state shapefile for context in map ##################################
state.df <- readOGR(dsn = "H:\\new_data_8_18_19\\e_drive\\data\\general\\sf", layer = "states_wgs84")






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
# coord_map(project="polyconic") +
  coord_equal(clip = 'off') +
  #### add title to map #######
labs(title = obj$title) +



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
    plot.title = element_text(size= 35, vjust=-12.0, hjust=0.20, color = "#4e4d47"),
    plot.caption = element_text(size= 18, color = "blue"),
    legend.position = c(0.12, 0.1)

  ) +



  ### create a discrete scale. These functions allow you to specify your own set of mappings from levels in the data to aesthetic values.
  scale_fill_manual(values = rev(brewer.pal(10, 'PRGn')[obj$legend_range]),
  # scale_fill_manual(values = custom_pallete,
                    ###legend labels
                    labels = '',

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



####add anotation to the map #####################################

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
    ggplot_object = annotation_custom(grobTree(textGrob(labels[i], x=cnt, y= -0.20, rot = 0,gp=gpar(col="#4e4d47", fontsize=45, fontface="bold"))))
    ggplot_object_list <- append(ggplot_object_list, list(ggplot_object))
    cnt = cnt + multiplier
    i = i + 1
  }
  return(ggplot_object_list)
  
}

legend_title = annotation_custom(grobTree(textGrob("Percent Expansion",  x=0.47, y= -0.05, rot = 0,gp=gpar(col="#4e4d47", fontsize=45, fontface="bold"))))
legendlabels_abandon <- getggplotObject(cnt = 0.24, multiplier = 0.125, slots = 4, labels = c("0.5","2.5","5.0","7.5"))

#### add annotation to map object ###################################################
# d + legendlabels_abandon + 
yo <- d + legendlabels_abandon + legend_title



###create panel image ######################
dir = "H:\\new_data_8_18_19\\d_drive\\synthesis\\s35\\extensification\\graphics\\"
fileout=paste(dir,"test",".png", sep="")

####the key to not getting a bit error is to change the dpi  ---- no difference between 500dpi and 300dpi
ggsave(fileout, width = 34, height = 38, dpi = 500)





