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
library(ggthemes) # theme_map()


rm(list = ls(all.names = TRUE)) #will clear all objects includes hidden objects.



######################################################################
###define parameters of the object you want to map ###################
######################################################################
parent = 'intensification'
child = 'net'
grandchild = 'n2o'





rootpath = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\synthesis'

json_synthesis_master = paste(rootpath, 'json\\synthesis_master.json', sep='\\')
jsondata <- fromJSON(file=json_synthesis_master)


figure_json = paste(rootpath, 'shemas\\seth\\json\\figure_json.json', sep='\\')
figure_obj<- fromJSON(file=figure_json)



# ### dimensions of the legend bins ###
# keywidth = figure_obj$intensification$main$col1$keywidth
# keyheigh = figure_obj$intensification$main$col1$keyheight
# 
# ### gap between legend bins ###
# gap = figure_obj$intensification$main$col1$gap


### legend labels








user <- "mbougie"
host <- '144.92.235.105'
port <- '5432'
password <- 'Mend0ta!'
dbname <- 'synthesis'

### Make the connection to database ######################################################################
con_synthesis <- dbConnect(PostgreSQL(), dbname = dbname, user = user, host = host, port=port, password = password)





###########################################################################################
#####get the dataframes###################################################################
###########################################################################################




#### bring in state shapefile for context in map ##################################
state.df <- readOGR(dsn = "H:\\new_data_8_18_19\\e_drive\\data\\general\\sf", layer = "states")


### Main query that all the datasets will reference #####################################################
getquery <- function(parent){
  if(parent == 'intensification'){
    query_ext <- 'SELECT
    "dataset".fips,
    "dataset".mean,
    ("dataset".mean * conversion_table.conv_factor)  as current_field,
    \'lookup\' as dataset,
    geom
    FROM
    intensification_11_20_2019."dataset"
    INNER JOIN spatial.counties
    ON "dataset".fips = counties.fips
    INNER JOIN misc.conversion_table ON \'lookup\' = conversion_table.intensification'
    
    
    print(query_ext)
    return(query_ext)
  }else if(parent == 'extensification'){
    print('inside postgres extensification function')
    query_ext <- 'SELECT
    "dataset".atlas_stco,
    "dataset".mean,
    ("dataset".mean * conversion_table.conv_factor)*\'inversion_coeff\'  as current_field,
    \'lookup\' as dataset,
    geom
    FROM
    extensification_seth."dataset"
    INNER JOIN spatial.counties
    ON "dataset".atlas_stco = counties.fips
    INNER JOIN misc.conversion_table ON \'lookup\' = conversion_table.extensification'
    print(query_ext)
    return(query_ext)
  }
}






############################################################################################
########################### start analysis ############################################################
##############################################################################################
# rm(jsondata)
# jsondata <- fromJSON(file=json_file)


### ext #####

if(parent == 'extensification'){
query_specific <- gsub("dataset",jsondata[[parent]][[child]][[grandchild]]$dataset,getquery(parent))
query_specific <- gsub("inversion_coeff",jsondata[[parent]][[child]][[grandchild]]$inversion_coeff,query_specific)
query_specific <- gsub("lookup",jsondata[[parent]][[child]][[grandchild]]$lookup,query_specific)
}else{
### ext #####
query_specific <- gsub("dataset",jsondata[[parent]][[child]][[grandchild]]$dataset,getquery(parent))
query_specific <- gsub("lookup",jsondata[[parent]][[child]][[grandchild]]$lookup,query_specific)
}

print(query_specific)

jsondata[[parent]][[child]][[grandchild]]$df <- get_postgis_query(con_synthesis,
                                                  query_specific,
                                                  geom_name = "geom")################################################################










###### pick the json-bbject you want to explore ####################
rm(obj)
obj = jsondata[[parent]][[child]][[grandchild]]
print(obj$dataset)






######################################################################
################ analysis of specific df ##############################
#######################################################################
# ggplot(data=chol, aes(chol$AGE)) + 
#   geom_histogram()

### get the spatial dtaframe that is stored in the object
mapa <- obj$df

temp <- mapa@data
hist(temp$current_field, 100)

###get descriptive stats
yo <- summary(temp$current_field)
print(yo)
min_test = min(temp$current_field)
max_test = max(temp$current_field)
q_1 = quantile(temp$current_field, 0.25)
q_2 = quantile(temp$current_field, 0.50)
q_3 = quantile(temp$current_field, 0.75)


###get histogram
hist(temp$current_field, 100)


breaks_coef = obj$bin_params$coef
breaks_lower_count = obj$bin_params$lower$count
breaks_coef = obj$bin_params$coef
kernel = obj$bin_params$kernel


createScaledVector <- function(kernel, count, inf, upper_lower) {
  i = 1
  temp_vector <- c(kernel)
  while(i < count){
    print('-----temp_vector----------')
    print(temp_vector)
    x = temp_vector[i] * obj$bin_params$coef
    print('--------------x--------------')
    print(x)
    print('--------------i--------------')
    print(i)
    if((i == (count-1)) & (inf=='true') & (upper_lower==-1) ){
      temp_vector <- c(temp_vector, -1e300)
    }
    else if((i == (count-1)) & (inf=='true') & (upper_lower==1) ){
      temp_vector <- c(temp_vector, 1e300)
    }
    else{
      temp_vector <- c(temp_vector, x)
    }
    i = i + 1
  }
  return(temp_vector)
}

vector_low = createScaledVector((-1*kernel), obj$bin_params$lower$count, obj$bin_params$lower$inf, upper_lower=-1)
vector_high = createScaledVector((1*kernel), obj$bin_params$upper$count, obj$bin_params$upper$inf, upper_lower=1)

bin_breaks <-c(sort(vector_low), c(0), vector_high)
print(bin_breaks)

# ##### arguments for the labels #########################
# # replace lower infinity
# labels = replace(obj$bin_breaks, obj$bin_breaks==-1e+300, paste(2*(nth(obj$bin_breaks, 2)), "+", sep=""))
# print(labels)
# # replace upper infinity
# labels = replace(labels, labels==1e+300, paste(2*(nth(obj$bin_breaks, -2)), "+", sep=""))
# print(labels)
# 



###get the number of labels
number_of_labels = length(labels)
print('---------number_of_labels-----------')
print(number_of_labels)




print(labels2)




# breaks = breaks * -1
# print(breaks)
labels = as.character(bin_breaks)
# labels = as.character(obj$bin_breaks[obj$bin_breaks != 0])
print(labels)

temp$bins = cut(temp$current_field, breaks= bin_breaks)
table(temp$bins)



########################################################################
##### modify dataframes #####################################
########################################################################
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
# 
# 
# hist(mapa.df$current_field, 100)
# 
# ###create new fill column with cut values returned from getFillValues() function
# # mapa.df$fill = getFillValues()
# 
# 
# 
# print(obj$bin_breaks)
# 
# mapa.df$fill = cut(mapa.df$current_field, breaks= obj$bin_breaks)
# table(mapa.df$fill)
# 
# 
# 
# #####create stuff for the legend here (abstract this into a fct soon)
# labels = obj$bin_breaks
# labels[labels == 1e300] <- ""
# labels[labels == -1e300] <- ""
# print(labels)
# 
# 
# 
# bins = length(labels)
# 
# legend_length = keywidth * (bins - 1)
# print(legend_length)
# 
# gaps_length = gap*(bins-2)
# print(bins)
# 
# 
# 
# start_node = (1 - (legend_length+gaps_length))/2
# print(start_node)
# 
# ####################################################################
# 
# 
# 
# #### function to get legend range vector for color pallette that is derived from breaks vector in json
# 
# getLegendRange <- function(breaks){
#   v <- 1:10
#   print(v)
#   print(breaks)
#   print('-------------------------')
# 
# 
# 
#   print(sum(breaks < 0))
#   ### for negative values
#   total = 5 - (sum(breaks < 0))
#   print(total)
#   if(total != 0){v <- head(v, -total)}
#   print(total)
#   print(v)
# 
# 
# 
# 
#   #### for postive values
#   total = 5 - (sum(breaks > 0))
#   if(total != 0){v <- v[!v %in% 1:total]}
# 
#   print('-------------------------')
#   print(v)
# }
# 
# getLegendRange(obj$bin_breaks)
# 
# 
# ########################################################################
# ##### create graphic ###################################################
# ########################################################################
# 
# d = ggplot() +
# 
#   ### state grey background ###########
# geom_polygon(
#   data=state.df,
#   aes(y=lat, x=long, group=group),
#   fill='#cccccc'
# ) +
# 
# 
#   ### county choropleth map ###########
# geom_polygon(
#   data=mapa.df,
#   ###Aesthetic tells ggplot which variables are being mapped to the x axis, y axis,
#   ###(and often other attributes of the graph, such as the color fill).
#   ###Intuitively, the aesthetic can be thought of as what you are graphing.
# 
#   ###y-axis of graph referencing lat column
#   ###x-axis of graph referencing long column
#   ###group tells ggplot that the data has explicit groups
#   ###fill color of features referencing fill column. Fill color is initially arbitrary (changing the color of fill will be addressed later in code)
#   aes(y=lat, x=long, group=group, fill = fill),
#   colour = '#cccccc',
#   size = 0.5
# ) +
# 
# 
# 
#   ### state boundary strokes ###########
# geom_polygon(
#   data=state.df,
#   aes(y=lat, x=long, group=group),
#   alpha=0,
#   colour='white',
#   size=0.5
# ) +
# 
# 
# 
#   # Equal scale cartesian coordinates
#   ####NOTE: besure to set clip to off or the grob annotation is clipped by the dimensions of the panel
#   coord_equal(clip = 'off') +
# 
#   #### add title to map #######
# labs(title = obj$title) +
# 
# 
# 
#   theme(
#     #### nulled attributes ##################
#     axis.text.x = element_blank(),
#     axis.title.x=element_blank(),
#     axis.text.y = element_blank(),
#     axis.title.y=element_blank(),
#     axis.ticks = element_blank(),
#     axis.line = element_blank(),
# 
#     panel.background = element_rect(fill = 'green', color = 'green'),
#     panel.grid.major = element_blank(),
#     # panel.margin=unit(-10, "cm"),
# 
# 
# 
# 
# 
# 
#     plot.background = element_rect(fill = NA, color = NA),
# 
#     ###extend bottom margin of plot to accomidate legend and grob annotation
#     plot.margin = unit(c(0, 0, 2, 0), "cm"),
#     # plot.margin = unit(c(0, 0, 10, 0), "cm"),
# 
#     #### modified attributes ########################
#     ##parameters for the map title
#     plot.title = element_text(size= 45, vjust=-12.0, hjust=0.10, color = "#4e4d47"),
#     ##shifts the entire legend (graphic AND labels)
#     legend.text = element_text(color='white', size=0),
#     legend.margin=margin(t = -0.1, unit='cm'),
#     ###sets legend to 0,0 versus center of map???
#     legend.justification = c(0,0),
#     legend.position = c(start_node, -0.05),   ####(horizontal, vertical)
#     ###spacing between legend bins
#     legend.spacing.x = unit(gap, 'npc')
#     # legend.spacing.x = unit(1.25, 'cm')
# 
#     ####legend labels
#     # plot.caption = element_text(size= 30, vjust=-0.9, hjust=0.070, color = "#4e4d47") ###title size/position/color
#   ) +
# 
#   
#   ###this is modifying the specifics of INSIDE the legend (i.e. the legends components that make up the legend)
#   ### create a discrete scale. These functions allow you to specify your own set of mappings from levels in the data to aesthetic values.
#   scale_fill_manual(values = rev(brewer.pal(10, 'PRGn')[getLegendRange(obj$bin_breaks)]),
#   # scale_fill_manual(values = rev(brewer.pal(10, 'BrBG')[obj$legend_range]),
# 
#                     #Legend type guide shows key (i.e., geoms) mapped onto values.
#                     guide = guide_legend( title='',
#                                           title.theme = element_text(
#                                             size = 0,
#                                             color = "#4e4d47",
#                                             vjust=0.0,
#                                             angle = 0
#                                           ),
#                                           # legend bin dimensions
#                                           keyheight = unit(keyheigh, units = "npc"),
#                                           keywidth = unit(keywidth, units = "npc"),
# 
#                                           # keyheight = unit(0.5, units = "cm"),
#                                           # keywidth = unit(3, units = "cm"),
# 
#                                           #legend elements position
#                                           label.position = "bottom",
#                                           title.position = 'top',
# 
#                                           #The desired number of rows of legends.
#                                           nrow=1
#                                           # byrow=TRUE
# 
#                     )
#   )
# 
# 
# 
# legendLabelCreator <- function(start_node, bins, labels){
# 
#   ###declare the empty list that will hold all the ggplot objects
#   ggplot_object_list <- list()
#   
#   
#   ######NOTE possibel source of error!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#   multiplier = keywidth + gap
# 
#   limit = start_node + (multiplier * bins)
#   print('limit:-----------')
#   print(limit)
# 
#   i = 1
#   # labels <- c("20%","40%","60%","80%",">80%")
#   while (start_node < limit) {
#     print('start_node---------------')
#     print(start_node)
#     # ggplot_object = annotation_custom(grobTree(textGrob(labels[i], x=start_node, y= -0.06, just="left", rot = -45,gp=gpar(col="#4e4d47", fontsize=45, fontface="bold"))))
#     # ggplot_object_list <- append(ggplot_object_list, list(ggplot_object))
#     # start_node = start_node + keywidth + gap
#     if(i == 0) {
#       ggplot_object = annotation_custom(grobTree(textGrob(labels[i], x=start_node+gap, y= -0.06, just="left", rot = -45,gp=gpar(col="#4e4d47", fontsize=45, fontface="bold"))))
#     } else {
#       ggplot_object = annotation_custom(grobTree(textGrob(labels[i], x=start_node-(gap/2), y= -0.06, just="left", rot = -45,gp=gpar(col="#4e4d47", fontsize=45, fontface="bold"))))
# 
#     }
#     ggplot_object_list <- append(ggplot_object_list, list(ggplot_object))
#     start_node = start_node + keywidth + gap
#     i = i + 1
#     
#   }
#   return(ggplot_object_list)
# 
# }
# 
# 
# 
# legend_title <- annotation_custom(grobTree(textGrob(obj$legend_title, x = start_node, y = 0.00, just="left", rot = 0,gp=gpar(col="#4e4d47", fontsize=45, fontface="bold"))))
# legendlabels <- legendLabelCreator(start_node = start_node, bins = bins, labels = labels)
# 
# 
# #### add annotation to map object ###################################################
# ggplot_obj = d + legend_title + legendlabels
# 
# 
# ###create panel image ######################
# dir = "H:\\new_data_8_18_19\\d_drive\\synthesis\\s35\\intensification\\schemas\\seth\\graphics\\"
# fileout=paste(dir,"test",".png", sep="")
# 
# ####the key to not getting a bit error is to change the dpi  ---- no difference between 500dpi and 300dpi
# ggsave(fileout, width = 34, height = 38, dpi = 500)
