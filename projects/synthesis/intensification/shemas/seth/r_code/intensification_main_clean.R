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

rm(list = ls(all.names = TRUE)) #will clear all objects includes hidden objects.
gc() #free up memrory and report the memory usage.

rootpath = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\synthesis\\intensification\\shemas\\seth'

##### link to scripts #####################################################
source(paste(rootpath, 'r_code\\intensification_maps_current_4panels.R', sep='\\'))

###### link to json files #################################################
json_file = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\synthesis\\intensification\\shemas\\seth\\json\\synthesis_master.json'
jsondata <- fromJSON(file=json_file)

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
getquery <- function(parent){
  if(parent == 'intensification'){
    print('inside postgres intensification function')
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







createObject <- function(parent, child, grandchild){
  obj <- jsondata[[parent]][[child]][[grandchild]]
  
  query_specific <- gsub("dataset",jsondata[[parent]][[child]][[grandchild]]$dataset,getquery(parent))
  query_specific <- gsub("inversion_coeff",jsondata[[parent]][[child]][[grandchild]]$inversion_coeff,query_specific)
  query_specific <- gsub("lookup",jsondata[[parent]][[child]][[grandchild]]$lookup,query_specific)
  
  print(query_specific)
  df <- get_postgis_query(con_synthesis,query_specific,geom_name = "geom")

  ggplot_obj = createMap(df, obj)
  
  return(ggplot_obj)
}




runMain <- function(obj){

  parent = "extensification"
  
  lay <- rbind(c(1,1,1,1,1,1,1),
               c(1,1,1,1,1,1,1),
               c(1,1,1,1,1,1,1),
               c(2,2,2,2,2,2,2),
               c(2,2,2,2,2,2,2),
               c(2,2,2,2,2,2,2))
  
  
  ggplot_object_list_col1 <- list()
  ggplot_object_list_col2 <- list()
  
  for(i in obj$col1){
    print(i)
    print(paste0("i$child: ", i$child))
    print(i$grandchild)
    print(paste0("i$grandchild: ", i$grandchild))
    ggplot_object <- createObject(parent=parent, child=i$child, grandchild=i$grandchild)
    ggplot_object_list_col1 <- append(ggplot_object_list_col1, list(ggplot_object))
  }
  
  for(i in obj$col2){
    print(i)
    print(paste0("i$child: ", i$child))
    print(i$grandchild)
    print(paste0("i$grandchild: ", i$grandchild))
    ggplot_object <- createObject(parent=parent, child=i$child, grandchild=i$grandchild)
    ggplot_object_list_col2 <- append(ggplot_object_list_col2, list(ggplot_object))
  }
  merge_ggplotlists <- list("col1" = ggplot_object_list_col1, "col2" = ggplot_object_list_col2)
  return(merge_ggplotlists)
  
}








###### link to json files #################################################

json_file = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\synthesis\\intensification\\shemas\\seth\\json\\synthesis_master.json'
jsondata <- fromJSON(file=json_file)



json_file = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\synthesis\\intensification\\shemas\\seth\\json\\figure_json.json'
figure_obj<- fromJSON(file=json_file)

figure_params = figure_obj$extensification$figure$eric
merge_ggplotlists <- runMain(figure_params)

print(merge_ggplotlists$col2)


lay <- rbind(c(1,1,1,1,1,1,1),
             c(1,1,1,1,1,1,1),
             c(2,2,2,2,2,2,2),
             c(2,2,2,2,2,2,2))



col1 <- arrangeGrob(merge_ggplotlists$col1[[1]], merge_ggplotlists$col1[[2]], layout_matrix = lay)
col2 <- arrangeGrob(merge_ggplotlists$col2[[1]], merge_ggplotlists$col2[[2]], layout_matrix = lay)
plot_grid(col1,col2, ncol = 2, rel_heights = c(1, .1))
fileout = 'H:\\new_data_8_18_19\\d_drive\\synthesis\\s35\\intensification\\schemas\\seth\\graphics\\test.png'
ggsave(fileout, width = 34, height = 32, dpi = 500)































