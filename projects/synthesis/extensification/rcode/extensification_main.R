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

rootpath = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\synthesis\\extensification'

#####link to the other two scripts
source(paste(rootpath, 'rcode\\extensification_maps.R', sep='\\'))

json_file = paste(rootpath, 'json\\json_panels.json', sep='\\')
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

######--------agroibis---------- ###########################################################################################################

### Expansion:attach df to specific object in json #####################################################
jsondata$agroibis$p_exp_imp_rfs$df <- get_postgis_query(con_synthesis, "SELECT p_exp_imp_rfs as current_field, st_transform(geom,4326) as geom FROM extensification_agroibis.agroibis_counties WHERE p_aban_imp_rfs IS NOT NULL",
                                                geom_name = "geom")

jsondata$agroibis$n_exp_imp_rfs$df <- get_postgis_query(con_synthesis, "SELECT n_exp_imp_rfs as current_field, st_transform(geom,4326) as geom FROM extensification_agroibis.agroibis_counties WHERE n_exp_imp_rfs IS NOT NULL",
                                                geom_name = "geom")

jsondata$agroibis$sed_exp_imp_rfs$df <- get_postgis_query(con_synthesis, "SELECT sed_exp_imp_rfs as current_field, st_transform(geom,4326) as geom FROM extensification_agroibis.agroibis_counties WHERE sed_exp_imp_rfs IS NOT NULL",
                                                geom_name = "geom")

jsondata$agroibis$et_exp_imp_rfs_gal$df <- get_postgis_query(con_synthesis, "SELECT et_exp_imp_rfs_gal as current_field, st_transform(geom,4326) as geom FROM extensification_agroibis.agroibis_counties WHERE et_exp_imp_rfs_gal IS NOT NULL",
                                                geom_name = "geom")



### Abandonment:attach df to specific object in json #####################################################
jsondata$agroibis$p_aban_imp_rfs$df <- get_postgis_query(con_synthesis, "SELECT p_aban_imp_rfs as current_field, st_transform(geom,4326) as geom FROM extensification_agroibis.agroibis_counties WHERE p_aban_imp_rfs IS NOT NULL",
                                                geom_name = "geom")

jsondata$agroibis$n_aban_imp_rfs$df <- get_postgis_query(con_synthesis, "SELECT n_aban_imp_rfs as current_field, st_transform(geom,4326) as geom FROM extensification_agroibis.agroibis_counties WHERE n_aban_imp_rfs IS NOT NULL",
                                                geom_name = "geom")

jsondata$agroibis$sed_aban_imp_rfs$df <- get_postgis_query(con_synthesis, "SELECT sed_aban_imp_rfs as current_field, st_transform(geom,4326) as geom FROM extensification_agroibis.agroibis_counties WHERE sed_aban_imp_rfs IS NOT NULL",
                                                geom_name = "geom")

jsondata$agroibis$et_aban_imp_rfs_gal$df <- get_postgis_query(con_synthesis, "SELECT et_aban_imp_rfs_gal as current_field, st_transform(geom,4326) as geom FROM extensification_agroibis.agroibis_counties WHERE et_aban_imp_rfs_gal IS NOT NULL",
                                                geom_name = "geom")



#######-------- mlra------------- ###########################################################################################################

### Expansion:attach df to specific object in json #####################################################
jsondata$mlra$ratio_expand_rfs_mlra$df <- get_postgis_query(con_synthesis, "SELECT ratio_expand_rfs_mlra as current_field, st_transform(geom,4326) as geom FROM extensification_mlra.extensification_mlra WHERE ratio_expand_rfs_mlra IS NOT NULL",
                                                geom_name = "geom")


### Abandonment:attach df to specific object in json #####################################################
jsondata$mlra$ratio_abandon_rfs_mlra$df <- get_postgis_query(con_synthesis, "SELECT ratio_abandon_rfs_mlra as current_field, st_transform(geom,4326) as geom FROM extensification_mlra.extensification_mlra WHERE ratio_abandon_rfs_mlra IS NOT NULL",
                                                geom_name = "geom")




#######-------- carbon------------- ###########################################################################################################

# ### Expansion:attach df to specific object in json #####################################################
# jsondata$carbon$e_gigagrams_co2e$df <- get_postgis_query(con_synthesis, "SELECT e_gigagrams_co2e as current_field, st_transform(geom,4326) as geom FROM extensification_mlra.extensification_mlra WHERE e_gigagrams_co2e IS NOT NULL",
#                                                 geom_name = "geom")
# 
# 
# ### Abandonment:attach df to specific object in json #####################################################
# jsondata$carbon$s_gigagrams_co2e$df <- get_postgis_query(con_synthesis, "SELECT s_gigagrams_co2e as current_field, st_transform(geom,4326) as geom FROM extensification_mlra.extensification_mlra WHERE s_gigagrams_co2e IS NOT NULL",
#                                                 geom_name = "geom")


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
 



############################################################################################
# # panel 1 ################################################################################
###########################################################################################


#### Get and store ggplot objects (i.e. the d objects created in the map script)
list_exp <- getggplotObject(list(jsondata$mlra$ratio_expand_rfs_mlra, jsondata$agroibis$et_exp_imp_rfs_gal, jsondata$agroibis$et_exp_imp_rfs_gal))
list_aban <- getggplotObject(list(jsondata$mlra$ratio_abandon_rfs_mlra, jsondata$agroibis$et_aban_imp_rfs_gal, jsondata$agroibis$et_aban_imp_rfs_gal))

###create panel image ######################
dir = "I:\\d_drive\\projects\\synthesis\\s35\\extensification\\graphics\\"
fileout=paste(dir,"extensification_panel_1",".png", sep="")

col1 = plot_grid(plotlist = list_exp, ncol = 1, nrow = 3, align = 'vh')
col2 = plot_grid(plotlist = list_aban, ncol = 1, nrow = 3, align = 'vh')

plot_grid(col1,col2, ncol = 2, rel_heights = c(1, .1))
####the key to not getting a bit error is to change the dpi  ---- no difference between 500dpi and 300dpi
ggsave(fileout, width = 34, height = 38, dpi = 500)




############################################################################################
# # panel 2 ################################################################################
###########################################################################################


#### Get and store ggplot objects (i.e. the d objects created in the map script)
list_exp <- getggplotObject(list(jsondata$agroibis$p_exp_imp_rfs, jsondata$agroibis$n_exp_imp_rfs, jsondata$agroibis$sed_exp_imp_rfs))
list_aban <- getggplotObject(list(jsondata$agroibis$p_aban_imp_rfs, jsondata$agroibis$n_aban_imp_rfs, jsondata$agroibis$sed_aban_imp_rfs))

###create panel image ######################
dir = "I:\\d_drive\\projects\\synthesis\\s35\\extensification\\graphics\\"
fileout=paste(dir,"extensification_panel_2",".png", sep="")

col1 = plot_grid(plotlist = list_exp, ncol = 1, nrow = 3, align = 'vh')
col2 = plot_grid(plotlist = list_aban, ncol = 1, nrow = 3, align = 'vh')

plot_grid(col1,col2, ncol = 2, rel_heights = c(1, .1))
####the key to not getting a bit error is to change the dpi  ---- no difference between 500dpi and 300dpi
ggsave(fileout, width = 34, height = 38, dpi = 500)



