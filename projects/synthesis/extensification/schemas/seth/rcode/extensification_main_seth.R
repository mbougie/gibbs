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

rootpath = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\synthesis\\extensification\\schemas\\seth'

#####link to the other two scripts
source(paste(rootpath, 'rcode\\extensification_maps_seth.R', sep='\\'))

json_file = paste(rootpath, 'json\\json_panels_seth.json', sep='\\')
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
geom
FROM
extensification_seth."dataset"
INNER JOIN spatial.counties
ON "dataset".atlas_stco = counties.fips
INNER JOIN misc.conversion_table ON \'lookup\' = conversion_table.extensification'




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


### regional area queries #####################################################
area_region_ext <- "SELECT * FROM extensification_seth.area_region_ext"

area_region_abd <- "SELECT * FROM extensification_seth.area_region_abd"



##### queries for GHG #########################
query_ghg_ext = "SELECT * FROM extensification_seth.query_ghg_ext"

query_ghg_abd = "SELECT * FROM extensification_seth.query_ghg_abd"

query_ghg_net = "SELECT * FROM extensification_seth.query_ghg_net"

#### remove json each time to refresh #############################

rm(jsondata)
jsondata <- fromJSON(file=json_file)




###########################################################################################
######--------area---------- ###########################################################################################################
###########################################################################################


############# region #############################################
### ext #####
query_specific <- gsub("dataset",jsondata$area$area_region_ext$dataset,area_region_ext)
query_specific <- gsub("lookup",jsondata$area$area_region_ext$lookup,query_specific)

print(query_specific)

jsondata$area$area_region_ext$df <- get_postgis_query(con_synthesis,
                                                  query_specific,
                                                  geom_name = "geom")

### abd #####
# rm(query_specific)
query_specific <- gsub("dataset",jsondata$area$area_region_abd$dataset,area_region_abd)
query_specific <- gsub("lookup",jsondata$area$area_region_abd$lookup,query_specific)

print(query_specific)

jsondata$area$area_region_abd$df <- get_postgis_query(con_synthesis,
                                                  query_specific,
                                                  geom_name = "geom")

yo <- jsondata$area$area_region_abd$df
############# county #############################################
### ext #####
query_specific <- gsub("dataset",jsondata$area$area_county_ext$dataset,query_ext)
query_specific <- gsub("lookup",jsondata$area$area_county_ext$lookup,query_specific)

print(query_specific)

jsondata$area$area_county_ext$df <- get_postgis_query(con_synthesis,
                                                      query_specific,
                                                      geom_name = "geom")

### abd #####
# rm(query_specific)
query_specific <- gsub("dataset",jsondata$area$area_county_abd$dataset,query_abd)
query_specific <- gsub("lookup",jsondata$area$area_county_abd$lookup,query_specific)

print(query_specific)

jsondata$area$area_county_abd$df <- get_postgis_query(con_synthesis,
                                                      query_specific,
                                                      geom_name = "geom")

### net #####
query_specific <- gsub("dataset",jsondata$area$area_county_net$dataset,query_ext)
query_specific <- gsub("lookup",jsondata$area$area_county_net$lookup,query_specific)

print(query_specific)

jsondata$area$area_county_net$df <- get_postgis_query(con_synthesis,
                                                      query_specific,
                                                      geom_name = "geom")


###########################################################################################
######--------agroibis---------- ###########################################################################################################
###########################################################################################


############# AET #############################################
### net #####
query_specific <- gsub("dataset",jsondata$agroibis$AET_net$dataset,query_ext)
query_specific <- gsub("lookup",jsondata$agroibis$AET_net$lookup,query_specific)

print(query_specific)

jsondata$agroibis$AET_net$df <- get_postgis_query(con_synthesis,
                                                  query_specific,
                                                  geom_name = "geom")


### ext #####
query_specific <- gsub("dataset",jsondata$agroibis$AET_ext$dataset,query_ext)
query_specific <- gsub("lookup",jsondata$agroibis$AET_ext$lookup,query_specific)

print(query_specific)

jsondata$agroibis$AET_ext$df <- get_postgis_query(con_synthesis,
                                                  query_specific,
                                                  geom_name = "geom")

### abd #####
# rm(query_specific)
query_specific <- gsub("dataset",jsondata$agroibis$AET_abd$dataset,query_abd)
query_specific <- gsub("lookup",jsondata$agroibis$AET_abd$lookup,query_specific)

print(query_specific)

jsondata$agroibis$AET_abd$df <- get_postgis_query(con_synthesis,
                                                  query_specific,
                                                  geom_name = "geom")



############# Irrig #############################################
### ext #####
query_specific <- gsub("dataset",jsondata$agroibis$Irrig_ext$dataset,query_ext)
query_specific <- gsub("lookup",jsondata$agroibis$Irrig_ext$lookup,query_specific)

print(query_specific)

jsondata$agroibis$Irrig_ext$df <- get_postgis_query(con_synthesis,
                                                    query_specific,
                                                    geom_name = "geom")

### abd #####
query_specific <- gsub("dataset",jsondata$agroibis$Irrig_abd$dataset,query_abd)
query_specific <- gsub("lookup",jsondata$agroibis$Irrig_abd$lookup,query_specific)

print(query_specific)

jsondata$agroibis$Irrig_abd$df <- get_postgis_query(con_synthesis,
                                                    query_specific,
                                                    geom_name = "geom")






############# NLeach #############################################

### net #####
query_specific <- gsub("dataset",jsondata$agroibis$NLeach_net$dataset,query_ext)
query_specific <- gsub("lookup",jsondata$agroibis$NLeach_net$lookup,query_specific)

print(query_specific)

jsondata$agroibis$NLeach_net$df <- get_postgis_query(con_synthesis,
                                                     query_specific,
                                                     geom_name = "geom")




### ext #####
query_specific <- gsub("dataset",jsondata$agroibis$NLeach_ext$dataset,query_ext)
query_specific <- gsub("lookup",jsondata$agroibis$NLeach_ext$lookup,query_specific)

print(query_specific)

jsondata$agroibis$NLeach_ext$df <- get_postgis_query(con_synthesis,
                                                     query_specific,
                                                     geom_name = "geom")

# ### abd #####
query_specific <- gsub("dataset",jsondata$agroibis$NLeach_abd$dataset,query_abd)
query_specific <- gsub("lookup",jsondata$agroibis$NLeach_abd$lookup,query_specific)

print(query_specific)

jsondata$agroibis$NLeach_abd$df <- get_postgis_query(con_synthesis,
                                                     query_specific,
                                                     geom_name = "geom")




############# SEDrunoff #############################################

### net #####
query_specific <- gsub("dataset",jsondata$agroibis$SEDrunoff_net$dataset,query_ext)
query_specific <- gsub("lookup",jsondata$agroibis$SEDrunoff_net$lookup,query_specific)

print(query_specific)

jsondata$agroibis$SEDrunoff_net$df <- get_postgis_query(con_synthesis,
                                                        query_specific,
                                                        geom_name = "geom")

### ext #####
query_specific <- gsub("dataset",jsondata$agroibis$SEDrunoff_ext$dataset,query_ext)
query_specific <- gsub("lookup",jsondata$agroibis$SEDrunoff_ext$lookup,query_specific)

print(query_specific)

jsondata$agroibis$SEDrunoff_ext$df <- get_postgis_query(con_synthesis,
                                                        query_specific,
                                                        geom_name = "geom")

### abd #####
query_specific <- gsub("dataset",jsondata$agroibis$SEDrunoff_abd$dataset,query_abd)
query_specific <- gsub("lookup",jsondata$agroibis$SEDrunoff_abd$lookup,query_specific)

print(query_specific)

jsondata$agroibis$SEDrunoff_abd$df <- get_postgis_query(con_synthesis,
                                                        query_specific,
                                                        geom_name = "geom")







############# TPrunoff #############################################
### net #####
query_specific <- gsub("dataset",jsondata$agroibis$TPrunoff_net$dataset,query_ext)
query_specific <- gsub("lookup",jsondata$agroibis$TPrunoff_net$lookup,query_specific)

print(query_specific)

jsondata$agroibis$TPrunoff_net$df <- get_postgis_query(con_synthesis,
                                                       query_specific,
                                                       geom_name = "geom")


### ext #####
query_specific <- gsub("dataset",jsondata$agroibis$TPrunoff_ext$dataset,query_ext)
query_specific <- gsub("lookup",jsondata$agroibis$TPrunoff_ext$lookup,query_specific)

print(query_specific)

jsondata$agroibis$TPrunoff_ext$df <- get_postgis_query(con_synthesis,
                                                       query_specific,
                                                       geom_name = "geom")

### abd #####
query_specific <- gsub("dataset",jsondata$agroibis$TPrunoff_abd$dataset,query_abd)
query_specific <- gsub("lookup",jsondata$agroibis$TPrunoff_abd$lookup,query_specific)

print(query_specific)

jsondata$agroibis$TPrunoff_abd$df <- get_postgis_query(con_synthesis,
                                                       query_specific,
                                                       geom_name = "geom")                                               




###########################################################################################
######------- carbon ---------- ###########################################################################################################
###########################################################################################


############# carbon #############################################
### ext #####
query_specific <- gsub("dataset",jsondata$carbon$carbon_ext$dataset,query_ext)
query_specific <- gsub("lookup",jsondata$carbon$carbon_ext$lookup,query_specific)

print(query_specific)

jsondata$carbon$carbon_ext$df <- get_postgis_query(con_synthesis,
                                                   query_specific,
                                                   geom_name = "geom")

### abd #####
query_specific <- gsub("dataset",jsondata$carbon$carbon_abd$dataset,query_abd)
query_specific <- gsub("lookup",jsondata$carbon$carbon_abd$lookup,query_specific)

print(query_specific)

jsondata$carbon$carbon_abd$df <- get_postgis_query(con_synthesis,
                                                   query_specific,
                                                   geom_name = "geom")

###########################################################################################
######------- ghg ---------- ###########################################################################################################
###########################################################################################
### ext #####
query_specific <- gsub("dataset",jsondata$ghg$ghg_ext$dataset,query_ghg_ext)
query_specific <- gsub("lookup",jsondata$ghg$ghg_ext$lookup,query_specific)

print(query_specific)

jsondata$ghg$ghg_ext$df <- get_postgis_query(con_synthesis,
                                             query_specific,
                                             geom_name = "geom")


### abd #####
query_specific <- gsub("dataset",jsondata$ghg$ghg_abd$dataset,query_ghg_abd)
query_specific <- gsub("lookup",jsondata$ghg$ghg_abd$lookup,query_specific)

print(query_specific)

jsondata$ghg$ghg_abd$df <- get_postgis_query(con_synthesis,
                                             query_specific,
                                             geom_name = "geom")


### net #####
query_specific <- gsub("dataset",jsondata$ghg$ghg_net$dataset,query_ghg_net)
query_specific <- gsub("lookup",jsondata$ghg$ghg_net$lookup,query_specific)

print(query_specific)

jsondata$ghg$ghg_net$df <- get_postgis_query(con_synthesis,
                                             query_specific,
                                             geom_name = "geom")

###########################################################################################
######------- n2o ---------- ###########################################################################################################
###########################################################################################


############# n2o #############################################
### ext #####
query_specific <- gsub("dataset",jsondata$n2o$n2o_ext$dataset,query_ext)
query_specific <- gsub("lookup",jsondata$n2o$n2o_ext$lookup,query_specific)

print(query_specific)

jsondata$n2o$n2o_ext$df <- get_postgis_query(con_synthesis,
                                             query_specific,
                                             geom_name = "geom")

### abd #####
query_specific <- gsub("dataset",jsondata$n2o$n2o_abd$dataset,query_abd)
query_specific <- gsub("lookup",jsondata$n2o$n2o_abd$lookup,query_specific)

print(query_specific)

jsondata$n2o$n2o_abd$df <- get_postgis_query(con_synthesis,
                                             query_specific,
                                             geom_name = "geom")



###########################################################################################
######------- napp ---------- ###########################################################################################################
###########################################################################################


############# napp #############################################
### ext #####
query_specific <- gsub("dataset",jsondata$napp$napp_ext$dataset,query_ext)
query_specific <- gsub("lookup",jsondata$napp$napp_ext$lookup,query_specific)

print(query_specific)

jsondata$napp$napp_ext$df <- get_postgis_query(con_synthesis,
                                               query_specific,
                                               geom_name = "geom")

### abd #####
query_specific <- gsub("dataset",jsondata$napp$napp_abd$dataset,query_abd)
query_specific <- gsub("lookup",jsondata$napp$napp_abd$lookup,query_specific)

print(query_specific)

jsondata$napp$napp_abd$df <- get_postgis_query(con_synthesis,
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
 



###########################################################################################
#### panel Main ###########################################################################
###########################################################################################
rm(createMap)
source(paste(rootpath, 'rcode\\extensification_maps_seth.R', sep='\\'))


#### Get and store ggplot objects (i.e. the d objects created in the map script)
list_net_col1 <- getggplotObject(list(jsondata$area$area_county_net, jsondata$agroibis$AET_net, jsondata$ghg$ghg_net))
# list_net_col2 <- getggplotObject(list(jsondata$area$area_county_net, jsondata$agroibis$AET_net, jsondata$ghg$ghg_net))
list_net_col2 <- getggplotObject(list(jsondata$agroibis$NLeach_net, jsondata$agroibis$SEDrunoff_net, jsondata$agroibis$TPrunoff_net))

###create panel image ######################
dir = "H:\\new_data_8_18_19\\d_drive\\synthesis\\s35\\extensification\\graphics\\"
fileout=paste(dir,"extensification_panel_main",".png", sep="")

col1 = plot_grid(plotlist = list_net_col1, ncol = 1, nrow = 3, align = 'vh')
col2 = plot_grid(plotlist = list_net_col2, ncol = 1, nrow = 3, align = 'vh')


plot_grid(col1,col2, ncol = 2, rel_heights = c(1, .1))


ggsave(fileout, width = 100, height = 127, units="cm", limitsize = FALSE, dpi = 300)






# ###########################################################################################
# #### supplemental panel 1 #################################################################
# ###########################################################################################
# rm(createMap)
# source(paste(rootpath, 'rcode\\extensification_maps_seth.R', sep='\\'))
# 
# #### Get and store ggplot objects (i.e. the d objects created in the map script)
# list_exp <- getggplotObject(list(jsondata$agroibis$NLeach_ext, jsondata$agroibis$SEDrunoff_ext, jsondata$agroibis$TPrunoff_ext))
# list_aban <- getggplotObject(list(jsondata$agroibis$NLeach_abd, jsondata$agroibis$SEDrunoff_abd, jsondata$agroibis$TPrunoff_abd))
# 
# ###create panel image ######################
# dir = "H:\\new_data_8_18_19\\d_drive\\synthesis\\s35\\extensification\\graphics\\"
# fileout=paste(dir,"extensification_panel_supplemental_1",".png", sep="")
# 
# col1 = plot_grid(plotlist = list_exp, ncol = 1, nrow = 3, align = 'vh')
# col2 = plot_grid(plotlist = list_aban, ncol = 1, nrow = 3, align = 'vh')
# 
# plot_grid(col1,col2, ncol = 2, rel_heights = c(1, .1))
# ####the key to not getting a bit error is to change the dpi  ---- no difference between 500dpi and 300dpi
# ggsave(fileout, width = 100, height = 127, units="cm", limitsize = FALSE, dpi = 300)
# 
# 
# 
# ###########################################################################################
# #### supplemental panel 2 #################################################################
# ###########################################################################################
# rm(createMap)
# source(paste(rootpath, 'rcode\\extensification_maps_seth.R', sep='\\'))
# 
# #### Get and store ggplot objects (i.e. the d objects created in the map script)
# list_exp <- getggplotObject(list(jsondata$carbon$carbon_ext, jsondata$n2o$n2o_ext, jsondata$ghg$ghg_ext))
# list_aban <- getggplotObject(list(jsondata$carbon$carbon_abd, jsondata$n2o$n2o_abd, jsondata$ghg$ghg_abd))
# 
# ###create panel image ######################
# dir = "H:\\new_data_8_18_19\\d_drive\\synthesis\\s35\\extensification\\graphics\\"
# fileout=paste(dir,"extensification_panel_supplemental_2",".png", sep="")
# 
# col1 = plot_grid(plotlist = list_exp, ncol = 1, nrow = 3, align = 'vh')
# col2 = plot_grid(plotlist = list_aban, ncol = 1, nrow = 3, align = 'vh')
# 
# plot_grid(col1,col2, ncol = 2, rel_heights = c(1, .1))
# ####the key to not getting a bit error is to change the dpi  ---- no difference between 500dpi and 300dpi
# ggsave(fileout, width = 100, height = 127, units="cm", limitsize = FALSE, dpi = 300)
# 
# 
# 
# ############################################################################################
# #### supplemental panel 3 ##################################################################
# ############################################################################################
# rm(createMap)
# source(paste(rootpath, 'rcode\\extensification_maps_seth.R', sep='\\'))
# 
# #### Get and store ggplot objects (i.e. the d objects created in the map script)
# list_exp <- getggplotObject(list(jsondata$area$area_region_ext, jsondata$area$area_county_ext, jsondata$agroibis$AET_ext))
# list_aban <- getggplotObject(list(jsondata$area$area_region_abd, jsondata$area$area_county_abd, jsondata$agroibis$AET_abd))
# 
# ###create panel image ######################
# dir = "H:\\new_data_8_18_19\\d_drive\\synthesis\\s35\\extensification\\graphics\\"
# fileout=paste(dir,"extensification_panel_supplemental_3",".png", sep="")
# 
# col1 = plot_grid(plotlist = list_exp, ncol = 1, nrow = 3, align = 'vh')
# col2 = plot_grid(plotlist = list_aban, ncol = 1, nrow = 3, align = 'vh')
# 
# plot_grid(col1,col2, ncol = 2, rel_heights = c(1, .1))
# ####the key to not getting a bit error is to change the dpi  ---- no difference between 500dpi and 300dpi
# ggsave(fileout, width = 100, height = 127, units="cm", limitsize = FALSE, dpi = 300)

