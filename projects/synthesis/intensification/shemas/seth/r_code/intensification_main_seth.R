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
 



############################################################################################
# # panel test ################################################################################
###########################################################################################


#### Get and store ggplot objects (i.e. the d objects created in the map script)
list_exp <- getggplotObject(list(jsondata$cc$phos,jsondata$oo$phos,jsondata$co$phos,jsondata$net$phos))


### get the grouped legend ##################################
# list_legends <- getggplotObject_map(list(json_legend$intensification$map$phos))
legend1 <- get_legend(createDummy(json_legend$intensification$map$phos) + theme(legend.position="bottom", legend.justification="center"))


###create panel image ######################
dir = "H:\\new_data_8_18_19\\d_drive\\synthesis\\s35\\intensification\\graphics\\"
fileout=paste(dir,"extensification_panel_test",".png", sep="")

col1 = plot_grid(plotlist = list_exp, ncol = 1, nrow = 4, align = 'vh')
# col2 = plot_grid(plotlist = list_aban, ncol = 1, nrow = 4, align = 'vh')

plot_grid(col1, legend1, ncol = 1, rel_heights = c(1, .1))
####the key to not getting a bit error is to change the dpi  ---- no difference between 500dpi and 300dpi
ggsave(fileout, width = 34, height = 38, dpi = 500)



# 
# plot_grid(col1, col2, legend1, legend2, ncol = 2, rel_heights = c(1, .1))





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

