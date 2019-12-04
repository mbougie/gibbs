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
library(ggthemes) # theme_map()

rootpath = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\synthesis\\extensification'

#####link to the other two scripts
source(paste(rootpath, 'rcode\\extensification_maps.R', sep='\\'))

json_file = paste(rootpath, 'schemas\\seth\\json\\json_panels_seth.json', sep='\\')

jsondata <- fromJSON(file=json_file)




#### bring in state shapefile for context in map ##################################
state.df <- readOGR(dsn = "H:\\new_data_8_18_19\\e_drive\\data\\general\\sf", layer = "states")





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
geom
FROM
extensification_seth."dataset"
INNER JOIN spatial.counties
ON "dataset".atlas_stco = counties.fips
INNER JOIN misc.conversion_table ON \'lookup\' = conversion_table.extensification'


query_ext_area_region <- 'SELECT
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


query_abd_area_region <- 'SELECT
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



query_ghg = "SELECT 
            n2o.atlas_stco, 
            n2o.mean,
            factor_n2o.conv_factor,
            n2o.mean * factor_n2o.conv_factor as n2o_Mg,
            carbon.atlas_stco, 
            carbon.mean,
            factor_carbon.conv_factor,
            carbon.mean * factor_carbon.conv_factor as carbon_Gg,
            
            ----Note: the unit for this column is Tg-----------------------
            ((n2o.mean * factor_n2o.conv_factor)/1000000) + ((carbon.mean * factor_carbon.conv_factor)/1000) as current_field,
            
            geom
            
            FROM 
            extensification_seth.net_diff_n2o_fips_summary as n2o INNER JOIN
            extensification_seth.net_diff_carbon_fips_summary as carbon
            USING(atlas_stco)
            INNER JOIN misc.conversion_table as factor_n2o
            ON factor_n2o.extensification = 'n2o'
            INNER JOIN misc.conversion_table as factor_carbon
            ON factor_carbon.extensification = 'carbon'
            INNER JOIN spatial.counties
            ON n2o.atlas_stco = counties.fips"





# #Legend type guide shows key (i.e., geoms) mapped onto values.
# guide = guide_legend( title='',
#                       title.theme = element_text(
#                         size = 0,
#                         color = "#4e4d47",
#                         vjust=0.0,
#                         angle = 0
#                       ),
#                       # legend bin dimensions
#                       keyheight = unit(0.015, units = "npc"),
#                       keywidth = unit(0.055, units = "npc"),
#                       
#                       #legend elements position
#                       label.position = "bottom",
#                       title.position = 'top',
#                       
#                       #The desired number of rows of legends.
#                       nrow=1
#                       # byrow=TRUE
#                       
# 





############### LEGEND ############################################################

## this is used to sort the bars by group name
# df2 <- aggregate(df$perc, by=list(name=df$name), FUN=sum)
# print(df2)
# 
# total <- merge(df,df2,by="name")
# total$group_name <- with(total, reorder(name, -x))
# print(total)
# 
# total2 <- total[!duplicated(total[,c('name','x')]),]
# print(total2)
# 
# legend <- ggplot(total, aes(x=name, y=acres, fill=name)) +
#   geom_bar(stat = "identity", width = 0.5)+
#   theme(aspect.ratio = 1/3,
#         legend.position="none",
#         axis.title.y=element_blank(),
#         axis.ticks.y=element_blank(),
#         panel.background = element_blank())+
#   labs(y="Acreage in Millions", x="meows")+
#   coord_flip()+
#   
#   geom_text(aes(label = paste0(perc,"%")), position=position_dodge(width=0.5), hjust= -0.4, vjust= 0.3, size=10, fontface="bold.italic") +
#   
#   scale_fill_manual(values = jColors)+
#   scale_y_continuous(labels=format_mil, expand = c(0, 0), limits = c(0, 4000000))




legend <- ggplot() +
  geom_tile(
    data = bivariate_color_scale,
    mapping = aes(
      x = gini,
      y = mean,
      fill = fill)
  ) +
  scale_fill_identity() +
  labs(x = "Higher inequality ??????",
       y = "Higher income ??????") +
  theme_map() +
  # make font small enough
  theme(
    axis.title = element_text(size = 6)
  ) +
  # quadratic tiles
  coord_fixed()








#################  Part 1 #########################################################

rm(jsondata)
jsondata <- fromJSON(file=json_file)

obj = jsondata$ghg$ghg
# obj = jsondata$agroibis$AET_net
# obj = jsondata$area_county$area_net


## ext and net #####
query_specific <- gsub("dataset",obj$dataset,query_ghg)
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



slots = length(labels)
print(slots)

legend_length = 0.060 * slots
print(legend_length)

cnt = (1 - legend_length)/2
print(cnt)




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


###create new fill column with cut values returned from getFillValues() function
# mapa.df$fill = getFillValues()
mapa.df$fill = cut(mapa.df$current_field, breaks= obj$bin_breaks)
table(mapa.df$fill)






########################################################################
##### create graphic ###################################################
########################################################################


#### graphics
d <- ggplot() + 
  

  
  ### state boundary background ###########
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
  
  
  
  # Equal scale cartesian coordinates
  ####NOTE: besure to set clip to off or the grob annotation is clipped by the dimensions of the panel
  coord_equal(clip = 'off') +
  
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
    # panel.margin=unit(-10, "cm"),
    
    
    
    
    
    
    plot.background = element_rect(fill = NA, color = NA),
    
    ###extend bottom margin of plot to accomidate legend and grob annotation
    plot.margin = unit(c(0, 0, 2, 0), "cm"),
    # plot.margin = unit(c(0, 0, 10, 0), "cm"),
    
    #### modified attributes ########################
    ##parameters for the map title
    plot.title = element_text(size= 45, vjust=-12.0, hjust=0.10, color = "#4e4d47"),
    ##shifts the entire legend (graphic AND labels)
    legend.text = element_text(color='white', size=0),
    legend.margin=margin(t = -0.1, unit='cm'),
    ###sets legend to 0,0 versus center of map???
    legend.justification = c(0,0),
    # legend.position = c(cnt, -0.00),   ####(horizontal, vertical)
    legend.position = "none",
    ###spacing between legend bins
    legend.spacing.x = unit(0.5, 'cm')
    
    ####legend labels
    # plot.caption = element_text(size= 30, vjust=-0.9, hjust=0.070, color = "#4e4d47") ###title size/position/color
  ) +
  
  
  ###this is modifying the specifics of INSIDE the legend (i.e. the legends components that make up the legend)
  ### create a discrete scale. These functions allow you to specify your own set of mappings from levels in the data to aesthetic values.
  scale_fill_manual(values = rev(brewer.pal(10, 'PRGn')[obj$legend_range]),
                    
                    #Legend type guide shows key (i.e., geoms) mapped onto values.
                    guide = guide_legend( title='',
                                          title.theme = element_text(
                                            size = 0,
                                            color = "#4e4d47",
                                            vjust=0.0,
                                            angle = 0
                                          ),
                                          # legend bin dimensions
                                          keyheight = unit(0.015, units = "npc"),
                                          keywidth = unit(0.055, units = "npc"),
                                          
                                          #legend elements position
                                          label.position = "bottom",
                                          title.position = 'top',
                                          
                                          #The desired number of rows of legends.
                                          nrow=1
                                          # byrow=TRUE
                                          
                    )
  )



getggplotObject <- function(cnt, multiplier, slots, labels){
  
  ###declare the empty list that will hold all the ggplot objects
  ggplot_object_list <- list()

  # length_of_legend = (multiplier * slots)
  # print(length_of_legend)
  
  limit = cnt + (multiplier * slots)
  print(limit)
  
  i = 1
  # labels <- c("20%","40%","60%","80%",">80%")
  while (cnt < limit) {
    print(cnt)
    ggplot_object = annotation_custom(grobTree(textGrob(labels[i], x=cnt, y= -0.01, just="left", rot = -45,gp=gpar(col="#4e4d47", fontsize=45, fontface="bold"))))
    ggplot_object_list <- append(ggplot_object_list, list(ggplot_object))
    cnt = cnt + multiplier
    i = i + 1
  }
  return(ggplot_object_list)
  
}






legend_title <- annotation_custom(grobTree(textGrob(obj$legend_title, x = 0.29, y = 0.050, rot = 0,gp=gpar(col="#4e4d47", fontsize=45, fontface="bold"))))
legendlabels <- getggplotObject(cnt = cnt, multiplier = 0.060, slots = length(labels), labels = labels)



#### add annotation to map object ###################################################
# d + legendlabels_abandon + 
yo <- d + legend_title + legendlabels






rects <- data.frame(x = 1:4,
                    colors = c("red", "green", "blue", "magenta"),
                    text = paste("text", 1:4))


p <- ggplot(rects, aes(x, y = 0, fill = colors, label = text)) +
  geom_tile(width = 0.25, height = .1) + # make square tiles
  geom_text(color = "white") + # add white text in the middle
  scale_fill_manual(values = rev(brewer.pal(10, 'PRGn'))) + # color the tiles with the colors in the data frame
  coord_fixed() + # make sure tiles are square

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
  legend.position = "none"
)

meow =p + legend_title + legendlabels
# ###create a matrix that will be filled with the plots above
lay <- rbind(c(1,1,1,1,1,1,1,1),
             c(1,1,1,1,1,1,1,1),
             c(1,1,1,1,1,1,1,1),
             c(1,1,1,1,1,1,1,1),
             c(1,1,1,1,1,1,1,1),
             c(1,1,1,1,1,1,1,1),
             c(1,1,2,2,2,2,1,1))
# 
# #merge all three plots within one grid (and visualize this)
g <- arrangeGrob(d,meow, layout_matrix = lay)


fileout = 'H:\\new_data_8_18_19\\d_drive\\synthesis\\s35\\extensification\\graphics\\test.png'
ggsave(fileout, width = 34, height = 25, dpi = 500, g)































