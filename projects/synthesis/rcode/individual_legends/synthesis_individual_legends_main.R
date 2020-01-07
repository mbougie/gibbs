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
library(stringi)

library(dplyr)

library(gridBase)
library(grid)
library(gridExtra) #load Grid

rm(list = ls(all.names = TRUE)) #will clear all objects includes hidden objects.
gc() #free up memrory and report the memory usage.

rootpath = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\synthesis'

##### link to scripts #####################################################
source(paste(rootpath, 'rcode\\individual_legends\\synthesis_individual_legends_map.R', sep='\\'))

###### link to json files #################################################
json_synthesis_master = paste(rootpath, 'json\\synthesis_master.json', sep='\\')
jsondata <- fromJSON(file=json_synthesis_master)


figure_json = paste(rootpath, 'json\\figure_json.json', sep='\\')
figure_obj<- fromJSON(file=figure_json)



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
getquery <- function(parent, state){
  if(parent == 'intensification'){
    print('inside postgres intensification function')
    query_ext <- 'SELECT
    "dataset".fips,
    "dataset".mean,
    ("dataset".mean * conversion_table.conv_factor)  as current_field,
    \'lookup\' as dataset,
    conversion_table.legend_title,
    geom
    FROM
    intensification_11_20_2019."dataset"
    INNER JOIN spatial.counties
    ON "dataset".fips = counties.fips
    INNER JOIN misc.conversion_table ON \'lookup\' = conversion_table.intensification'
    
    
    print(query_ext)
    return(query_ext)
    
  }else if(parent == 'extensification'){
    
    if(state==FALSE){
      print('inside postgres extensification function')
      query_ext <- 'SELECT
      "dataset".atlas_stco,
      "dataset".mean,
      ("dataset".mean * conversion_table.conv_factor)*\'inversion_coeff\'  as current_field,
      \'lookup\' as dataset,
      conversion_table.legend_title,
      geom
      FROM
      extensification_seth."dataset"
      INNER JOIN spatial.counties
      ON "dataset".atlas_stco = counties.fips
      INNER JOIN misc.conversion_table ON \'lookup\' = conversion_table.extensification'
      print(query_ext)
      return(query_ext)
    }
    else if(state==TRUE){
      query_ext <- 'SELECT * FROM extensification_seth."dataset"'
      print(query_ext)
      return(query_ext)
    }
  }
}







createObject <- function(figure_params, parent, child, grandchild, title){
  obj <- jsondata[[parent]][[child]][[grandchild]]
  print('obj$inversion_coeff')
  print(obj$inversion_coeff)
  print(is.null(obj[["inversion_coeff"]]))
  if(parent == 'extensification'){
    if(is.null(obj[["inversion_coeff"]])){
      query_specific <- gsub("dataset",jsondata[[parent]][[child]][[grandchild]]$dataset,getquery(parent, is.null(obj[["inversion_coeff"]])))
    }
    else{
    query_specific <- gsub("dataset",jsondata[[parent]][[child]][[grandchild]]$dataset,getquery(parent, is.null(obj[["inversion_coeff"]])))
    query_specific <- gsub("inversion_coeff",jsondata[[parent]][[child]][[grandchild]]$inversion_coeff,query_specific)
    query_specific <- gsub("lookup",jsondata[[parent]][[child]][[grandchild]]$lookup,query_specific)
    }

  }else{
    query_specific <- gsub("dataset",jsondata[[parent]][[child]][[grandchild]]$dataset,getquery(parent, is.null(obj[["inversion_coeff"]])))
    query_specific <- gsub("lookup",jsondata[[parent]][[child]][[grandchild]]$lookup,query_specific) 
  }
  print(query_specific)
  df <- get_postgis_query(con_synthesis,query_specific,geom_name = "geom")

  ggplot_obj = createMap(df, obj, figure_params, title)
  # 
  # return(ggplot_obj)
}



runMain <- function(parent, figure_params){

 
  
  ggplot_object_list_col1 <- list()
  ggplot_object_list_col2 <- list()
  
  for(i in figure_params$col1){
    print(i)
    print(paste0("i$child: ", i$child))
    print(i$grandchild)
    print(paste0("i$grandchild: ", i$grandchild))
    ggplot_object <- createObject(figure_params, parent=parent, child=i$child, grandchild=i$grandchild, title=i$title)
    ggplot_object_list_col1 <- append(ggplot_object_list_col1, list(ggplot_object))
  }
  
  for(i in figure_params$col2){
    print(i)
    print(paste0("i$child: ", i$child))
    print(i$grandchild)
    print(paste0("i$grandchild: ", i$grandchild))
    ggplot_object <- createObject(figure_params, parent=parent, child=i$child, grandchild=i$grandchild, title=i$title)
    ggplot_object_list_col2 <- append(ggplot_object_list_col2, list(ggplot_object))
  }
  merge_ggplotlists <- list("col1" = ggplot_object_list_col1, "col2" = ggplot_object_list_col2)
  return(merge_ggplotlists)
  
}









###### link to json files #################################################
# rm(figure_obj)
json_file = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\synthesis\\json\\synthesis_master.json'
jsondata <- fromJSON(file=json_file)



json_file = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\synthesis\\json\\figure_json.json'
figure_obj<- fromJSON(file=json_file)

parent="extensification"
figure="main"

####DEFINE THE FIGURE OBJECT HERE!!!!!!!!!!!!!!!!!!!
figure_params = figure_obj[[parent]][[figure]]
merge_ggplotlists <- runMain(parent = parent, figure_params)

lay <- rbind(c(1,1,1,1,1,1,1),
             c(1,1,1,1,1,1,1),
             c(2,2,2,2,2,2,2),
             c(2,2,2,2,2,2,2),
             c(3,3,3,3,3,3,3),
             c(3,3,3,3,3,3,3))



col1 <- arrangeGrob(merge_ggplotlists$col1[[1]], merge_ggplotlists$col1[[2]], merge_ggplotlists$col1[[3]], layout_matrix = lay)
col2 <- arrangeGrob(merge_ggplotlists$col2[[1]], merge_ggplotlists$col2[[2]], merge_ggplotlists$col2[[3]], layout_matrix = lay)
plot_grid(col1,col2, ncol = 2, rel_heights = c(1, .1))
# fileout = 'H:\\new_data_8_18_19\\d_drive\\synthesis\\s35\\deliverables\\graphics\\int\\figure.png'
fileout = gsub("figure", figure, "H:\\new_data_8_18_19\\d_drive\\synthesis\\s35\\deliverables\\graphics\\int\\figure.png")
ggsave(fileout, width = 34, height = 45, dpi = 500)
































