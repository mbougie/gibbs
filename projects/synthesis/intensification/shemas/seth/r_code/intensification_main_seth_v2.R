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


library(dplyr)



library(gridBase)
library(grid)
library(gridExtra) #load Grid

rootpath = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\synthesis\\intensification\\shemas\\seth'

#####link to the other two scripts
source(paste(rootpath, 'r_code\\intensification_maps_seth.R', sep='\\'))
source(paste(rootpath, 'r_code\\graphics_dummy_legend.R', sep='\\'))

json_file = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\synthesis\\intensification\\shemas\\seth\\json\\synthesis_intensification_seth.json'
jsondata <- fromJSON(file=json_file)


json_legend_file = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\synthesis\\intensification\\shemas\\seth\\json\\json_legends.json'
json_legend <- fromJSON(file=json_legend_file)



user <- "mbougie"
host <- '144.92.235.105'
port <- '5432'
password <- 'Mend0ta!'

### Make the connection to database ######################################################################
con_synthesis <- dbConnect(PostgreSQL(), dbname = 'synthesis', user = user, host = host, port=port, password = password)


rm(jsondata)
jsondata <- fromJSON(file=json_file)


###########################################################################################
#####get the dataframes###################################################################
###########################################################################################

### Main query that all the datasets will reference #####################################################
query_ext <- 'SELECT
              "dataset".fips,
              "dataset".mean,
              ("dataset".mean * conversion_table.conv_factor)  as current_field,
              \'lookup\' as dataset,
              st_transform(geom,4326) as geom
              FROM
              intensification_11_20_2019."dataset"
              INNER JOIN spatial.counties
              ON "dataset".fips = counties.fips
              INNER JOIN misc.conversion_table ON \'lookup\' = conversion_table.intensification'


print(query_ext)



###########################################################################################
######--------agroibis---------- ###########################################################################################################
###########################################################################################


############# CC #############################################
### ext #####
query_specific <- gsub("dataset",jsondata$cc$phos$dataset,query_ext)
query_specific <- gsub("lookup",jsondata$cc$phos$lookup,query_specific)

print(query_specific)

jsondata$cc$phos$df <- get_postgis_query(con_synthesis,
                                                  query_specific,
                                                  geom_name = "geom")



############# OO #############################################
### ext #####
query_specific <- gsub("dataset",jsondata$oo$phos$dataset,query_ext)
query_specific <- gsub("lookup",jsondata$oo$phos$lookup,query_specific)

print(query_specific)

jsondata$oo$phos$df <- get_postgis_query(con_synthesis,
                                                  query_specific,
                                                  geom_name = "geom")



############# CO #############################################
### ext #####
query_specific <- gsub("dataset",jsondata$co$phos$dataset,query_ext)
query_specific <- gsub("lookup",jsondata$co$phos$lookup,query_specific)

print(query_specific)

jsondata$co$phos$df <- get_postgis_query(con_synthesis,
                                                  query_specific,
                                                  geom_name = "geom")

############# NET #############################################
### ext #####
query_specific <- gsub("dataset",jsondata$net$phos$dataset,query_ext)
query_specific <- gsub("lookup",jsondata$net$phos$lookup,query_specific)

print(query_specific)

jsondata$net$phos$df <- get_postgis_query(con_synthesis,
                                                  query_specific,
                                                  geom_name = "geom")




######################################################################################################
############  get ggplot objects #####################################################################
######################################################################################################

getggplotObject <- function(obj_vector){
  
  ###declare the empty list that will hold all the ggplot objects
  ggplot_object_list <- list()
  
  for (obj in obj_vector){
    ggplot_object = createMap(obj)
    ggplot_object_list <- append(ggplot_object_list, list(ggplot_object))
  }
  return(ggplot_object_list)
  
}









######################################################################################################
############  get group legend #####################################################################
######################################################################################################

getggplotObject_map <- function(obj_vector){
  
  ###declare the empty list that will hold all the ggplot objects
  ggplot_object_list <- list()
  
  for (obj in obj_vector){
    ggplot_object = createDummy(obj)
    ggplot_object_list <- append(ggplot_object_list, list(ggplot_object))
  }
  return(ggplot_object_list)
  
}
 
# 
# ############################################################################################
# # # create legend REMOVE SOON !!!!!!!!!!!!!!!!!!!!!!!!!!! ################################################################################
# ###########################################################################################
# 
# createDummy_v2 <- function(obj){
#   print('start inside createDummy function---------------------------------------')
#   print(obj)
#   
#   onePrior = function(x){
#     sign = x/abs(x)
#     if(is.nan(sign)){
#       return(x)
#     }else{
#       return(sign*(abs(x)-1))
#     }
#   }
#   
#   vec = sapply(obj$bin_breaks, onePrior)
#   
#   df = data.frame(cbind(x = vec, y = vec))
#   
#   df['cuts'] = cut(df$x, breaks= obj$bin_breaks)
#   df = df[which(df$x != 0),]  # JANKALERT
#   
#   print(df)
#   
#   
#   #####create stuff for the legend here (abstract this into a fct soon)
#   labels = obj$bin_breaks
#   # labels[labels == 1e300] <- ""
#   # labels[labels == -1e300] <- ""
#   # print(labels)
#   
#   
#   
#   slots = length(labels)
#   print(slots)
#   
#   legend_length = 0.060 * slots
#   print(legend_length)
#   
#   cnt = (1 - legend_length)/2
#   print(cnt)
#   
#   
#   
#   
#   #### function to get legend range vectro for color pallette that is derived from breaks vector in json
#   
#   getLegendRange <- function(breaks){
#     v <- 1:10
#     print(v)
#     print(breaks)
#     print('-------------------------')
#     
#     
#     
#     print(sum(breaks < 0))
#     ### for negative values
#     total = 5 - (sum(breaks < 0))
#     print(total)
#     if(total != 0){v <- head(v, -total)}
#     print(total)
#     print(v)
#     
#     
#     
#     
#     #### for postive values
#     total = 5 - (sum(breaks > 0))
#     if(total != 0){v <- v[!v %in% 1:total]}
#     
#     print('-------------------------')
#     print(v)
#   }
#   
#   getLegendRange(obj$bin_breaks)
#   
#   
#   
#   d = ggplot(df, aes(x = x, y = y, fill = cuts)) +
#     geom_area() +
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
#       # panel.margin=unit(-10, "cm"),
#       
#       
#       
#       
#       
#       
#       plot.background = element_rect(fill = NA, color = NA),
#       
#       ###extend bottom margin of plot to accomidate legend and grob annotation
#       plot.margin = unit(c(0, 0, 2, 0), "cm"),
#       # plot.margin = unit(c(0, 0, 10, 0), "cm"),
#       
#       #### modified attributes ########################
#       ##parameters for the map title
#       plot.title = element_text(size= 45, vjust=-12.0, hjust=0.10, color = "#4e4d47"),
#       ##shifts the entire legend (graphic AND labels)
#       legend.text = element_text(color='white', size=0),
#       legend.margin=margin(t = -0.1, unit='cm'),
#       ###sets legend to 0,0 versus center of map???
#       legend.justification = c(0,0),
#       legend.position = c(0.25, 0.5),   ####(horizontal, vertical)
#       ###spacing between legend bins
#       # legend.spacing.x = unit(0.025, 'npc')
#       legend.spacing.x = unit(1.25, 'cm')
#       
#       ####legend labels
#       # plot.caption = element_text(size= 30, vjust=-0.9, hjust=0.070, color = "#4e4d47") ###title size/position/color
#     ) +
#     
#     
#     ###this is modifying the specifics of INSIDE the legend (i.e. the legends components that make up the legend)
#     ### create a discrete scale. These functions allow you to specify your own set of mappings from levels in the data to aesthetic values.
#     scale_fill_manual(values = rev(brewer.pal(10, 'PRGn')[getLegendRange(obj$bin_breaks)]),
#                       
#                       #Legend type guide shows key (i.e., geoms) mapped onto values.
#                       guide = guide_legend( title='',
#                                             title.theme = element_text(
#                                               size = 0,
#                                               color = "#4e4d47",
#                                               vjust=0.0,
#                                               angle = 0
#                                             ),
#                                             # legend bin dimensions
#                                             keyheight = unit(0.2, units = "npc"),
#                                             keywidth = unit(0.2, units = "npc"),
#                                             
#                                             # keyheight = unit(0.4, units = "cm"),
#                                             # keywidth = unit(3, units = "cm"),
#                                             
#                                             #legend elements position
#                                             label.position = "bottom",
#                                             title.position = 'top',
#                                             
#                                             #The desired number of rows of legends.
#                                             nrow=1
#                                             # byrow=TRUE
#                                             
#                       )
#     )
#   
#   
#   
#   getggplotObject <- function(cnt, multiplier, slots, labels){
# 
#     ###declare the empty list that will hold all the ggplot objects
#     ggplot_object_list <- list()
# 
#     # length_of_legend = (multiplier * slots)
#     # print(length_of_legend)
# 
#     limit = cnt + (multiplier * slots)
#     print(limit)
# 
#     i = 1
#     # labels <- c("20%","40%","60%","80%",">80%")
#     while (cnt < limit) {
#       print(cnt)
#       ggplot_object = annotation_custom(grobTree(textGrob(labels[i], x=cnt, y= -0.06, just="left", rot = -45,gp=gpar(col="#4e4d47", fontsize=45, fontface="bold"))))
#       ggplot_object_list <- append(ggplot_object_list, list(ggplot_object))
#       cnt = cnt + multiplier
#       i = i + 1
#     }
#     return(ggplot_object_list)
# 
#   }
# 
# 
# 
# 
# 
# 
#   # legend_title <- annotation_custom(grobTree(textGrob(obj$legend_title, x = 0.0, y = 0.0, just="left", rot = 0,gp=gpar(col="#4e4d47", fontsize=45, fontface="bold"))))
#   # legend_title <- annotation_custom(grobTree('meow', x = 0.1, y = 0.25, just="left", rot = 0,gp=gpar(col="#4e4d47", fontsize=45, fontface="bold")))
#   # legendlabels <- getggplotObject(cnt = 0.5, multiplier = 0.20, slots = length(labels), labels = labels)
#   # 
#   # 
#   # ggplot_obj = d + legend_title
# 
#   
#   return(legend_title)
#   
# }
############################################################################################
# # panel test ################################################################################
###########################################################################################


#### Get and store ggplot objects (i.e. the d objects created in the map script)
# list_exp <- getggplotObject(list(jsondata$cc$phos,jsondata$oo$phos,jsondata$co$phos,jsondata$net$phos))
p1 = createMap(jsondata$cc$phos)
p2 = createMap(jsondata$oo$phos)
p3 = createMap(jsondata$co$phos)
p4 = createMap(jsondata$net$phos)


### get the grouped legend ##################################
####NOTE: before to remove the legend object and refresh the function if changes are made to it!!!!!!!!
rm(legend2)
source(paste(rootpath, 'r_code\\graphics_dummy_legend.R', sep='\\'))

####create legend object
legend2 <- createDummy(json_legend$intensification$map$phos)

# ###create a matrix that will be filled with the plots above
lay <- rbind(c(1,1,1,1,1,1,1),
             c(1,1,1,1,1,1,1),
             c(2,2,2,2,2,2,2),
             c(2,2,2,2,2,2,2),
             c(3,3,3,3,3,3,3),
             c(3,3,3,3,3,3,3),
             c(4,4,4,4,4,4,4),
             c(4,4,4,4,4,4,4),
             c(5,5,5,5,5,5,5))
# 
# #merge all three plots within one grid (and visualize this)
g <- arrangeGrob(p1, p2, p3, p4, legend2, layout_matrix = lay)


fileout = 'H:\\new_data_8_18_19\\d_drive\\synthesis\\s35\\intensification\\graphics\\test.png'
ggsave(fileout, width = 34, height = 40, dpi = 500, g)





# ############################################################################################
# # # panel 1 ################################################################################
# ###########################################################################################
# 
# 
# #### Get and store ggplot objects (i.e. the d objects created in the map script)
# list_exp <- getggplotObject(list(jsondata$agroibis$AET_ext, jsondata$agroibis$Irrig_ext, jsondata$carbon$carbon_ext))
# list_aban <- getggplotObject(list(jsondata$agroibis$AET_abd, jsondata$agroibis$Irrig_abd, jsondata$carbon$carbon_abd))
# 
# ###create panel image ######################
# dir = "H:\\new_data_8_18_19\\d_drive\\synthesis\\s35\\intensification\\graphics\\"
# fileout=paste(dir,"intensification_panel_1",".png", sep="")
# 
# col1 = plot_grid(plotlist = list_exp, ncol = 1, nrow = 3, align = 'vh')
# col2 = plot_grid(plotlist = list_aban, ncol = 1, nrow = 3, align = 'vh')
# 
# plot_grid(col1,col2, ncol = 2, rel_heights = c(1, .1))
# ####the key to not getting a bit error is to change the dpi  ---- no difference between 500dpi and 300dpi
# ggsave(fileout, width = 34, height = 38, dpi = 500)
# 
# 
# 
# 
# ############################################################################################
# # # panel 2 ################################################################################
# ###########################################################################################
# 
# 
# #### Get and store ggplot objects (i.e. the d objects created in the map script)
# list_exp <- getggplotObject(list(jsondata$agroibis$TPrunoff_ext, jsondata$agroibis$NLeach_ext, jsondata$agroibis$SEDrunoff_ext))
# list_aban <- getggplotObject(list(jsondata$agroibis$TPrunoff_abd, jsondata$agroibis$NLeach_abd, jsondata$agroibis$SEDrunoff_abd))
# 
# ###create panel image ######################
# dir = "H:\\new_data_8_18_19\\d_drive\\synthesis\\s35\\intensification\\graphics\\"
# fileout=paste(dir,"intensification_panel_2",".png", sep="")
# 
# col1 = plot_grid(plotlist = list_exp, ncol = 1, nrow = 3, align = 'vh')
# col2 = plot_grid(plotlist = list_aban, ncol = 1, nrow = 3, align = 'vh')
# 
# plot_grid(col1,col2, ncol = 2, rel_heights = c(1, .1))
# ####the key to not getting a bit error is to change the dpi  ---- no difference between 500dpi and 300dpi
# ggsave(fileout, width = 34, height = 38, dpi = 500)
# 
# 
# 
# 
# 
# ############################################################################################
# # # panel 3 ################################################################################
# ###########################################################################################
# 
# 
# #### Get and store ggplot objects (i.e. the d objects created in the map script)
# list_exp <- getggplotObject(list(jsondata$napp$napp_ext, jsondata$n2o$n2o_ext))
# list_aban <- getggplotObject(list(jsondata$napp$napp_abd, jsondata$n2o$n2o_abd))
# 
# ###create panel image ######################
# dir = "H:\\new_data_8_18_19\\d_drive\\synthesis\\s35\\intensification\\graphics\\"
# fileout=paste(dir,"intensification_panel_3",".png", sep="")
# 
# col1 = plot_grid(plotlist = list_exp, ncol = 1, nrow = 2, align = 'vh')
# col2 = plot_grid(plotlist = list_aban, ncol = 1, nrow = 2, align = 'vh')
# 
# plot_grid(col1,col2, ncol = 2, rel_heights = c(1, .1))
# ####the key to not getting a bit error is to change the dpi  ---- no difference between 500dpi and 300dpi
# ggsave(fileout, width = 34, height = 38, dpi = 500)
# 

