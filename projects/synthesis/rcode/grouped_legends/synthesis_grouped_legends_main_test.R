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


###########################################################################################
##### load external data
###########################################################################################

rootpath = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\synthesis'

##### link to scripts #####################################################
source(paste(rootpath, 'rcode\\grouped_legends\\synthesis_grouped_legends_map.R', sep='\\'))
source(paste(rootpath, 'rcode\\grouped_legends\\graphics_dummy_legend.R', sep='\\'))

###### link to json files #################################################
json_synthesis_master = paste(rootpath, 'json\\synthesis_master.json', sep='\\')
jsondata <- fromJSON(file=json_synthesis_master)

figure_json = paste(rootpath, 'json\\figure_json.json', sep='\\')
figure_obj<- fromJSON(file=figure_json)

json_legend_file = paste(rootpath, 'json\\dummy_legends.json', sep='\\')
json_dummy<- fromJSON(file=json_legend_file)

###### connect to postgres database #################################################
user <- "mbougie"
host <- '144.92.235.105'
port <- '5432'
password <- 'Mend0ta!'

### Make the connection to database ######################################################################
con_synthesis <- dbConnect(PostgreSQL(), dbname = 'synthesis', user = user, host = host, port=port, password = password)













###### test area ###################################################################




###### test area ###################################################################
























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



#####refine this function
createObject <- function(figure_params, parent, panel_params){
  child=panel_params$child
  grandchild=panel_params$grandchild
  obj_params <- jsondata[[parent]][[child]][[grandchild]]

  if(parent == 'extensification'){
    if(is.null(obj_params[["inversion_coeff"]])){
      query_specific <- gsub("dataset",jsondata[[parent]][[child]][[grandchild]]$dataset,getquery(parent, is.null(obj_params[["inversion_coeff"]])))
    }
    else{
      query_specific <- gsub("dataset",jsondata[[parent]][[child]][[grandchild]]$dataset,getquery(parent, is.null(obj_params[["inversion_coeff"]])))
      query_specific <- gsub("inversion_coeff",jsondata[[parent]][[child]][[grandchild]]$inversion_coeff,query_specific)
      query_specific <- gsub("lookup",jsondata[[parent]][[child]][[grandchild]]$lookup,query_specific)
    }
    
  }else{
    query_specific <- gsub("dataset",jsondata[[parent]][[child]][[grandchild]]$dataset,getquery(parent, is.null(obj_params[["inversion_coeff"]])))
    query_specific <- gsub("lookup",jsondata[[parent]][[child]][[grandchild]]$lookup,query_specific) 
  }
  print(query_specific)
  df <- get_postgis_query(con_synthesis,query_specific,geom_name = "geom")
  
  ggplot_obj = createMap(df, obj_params, panel_params, figure_params)
  
  return(ggplot_obj)
}


#####rename this function and then comment
maptest <- function(parent, figure_params, x){
  ggplot_object_list_col <- list()
  
  col = figure_params$columns[[x]]
  #### get the panel names within each column
  for(i in names(col$panels)){
    ggplot_object <- createObject(figure_params, parent=parent, col$panels[[i]])
    ggplot_object_list_col <- append(ggplot_object_list_col, list(ggplot_object))
    

  }
  return(ggplot_object_list_col)
}


runMain <- function(parent, figure_params){

  merge_ggplotlists <- list()
  group_legends <- list()
  #### get the columns names within the object
  for(x in names(figure_params$columns)){
    
    #### get map objects
    merge_ggplotlists[[x]] <- maptest(parent, figure_params, x)
    
    ###get the legend objects
    group_legends[[x]] <- createGroupLegend(figure_params, x)
  }

  map_legend_objects <- list("maps" = merge_ggplotlists, "legends" = group_legends)
  return(map_legend_objects)
}




####DEFINE THE FIGURE OBJECT HERE!!!!!!!!!!!!!!!!!!!

figure_params = figure_obj$intensification$sup1
figure_objects <- runMain(parent = "intensification", figure_params)

 
lay <- rbind(c(1,1,1,1,1,1,1),
             c(1,1,1,1,1,1,1),
             c(2,2,2,2,2,2,2),
             c(2,2,2,2,2,2,2),
             c(3,3,3,3,3,3,3),
             c(3,3,3,3,3,3,3),
             c(4,4,4,4,4,4,4),
             c(4,4,4,4,4,4,4),
             c(5,5,5,5,5,5,5))




col1 <- arrangeGrob(figure_objects$maps$col1[[1]], figure_objects$maps$col1[[2]], figure_objects$maps$col1[[3]], figure_objects$maps$col1[[4]], figure_objects$legends$col1, layout_matrix = lay)
col2 <- arrangeGrob(figure_objects$maps$col2[[1]], figure_objects$maps$col2[[2]], figure_objects$maps$col2[[3]], figure_objects$maps$col2[[4]], figure_objects$legends$col2, layout_matrix = lay)
plot_grid(col1,col2, ncol = 2, rel_heights = c(1, .1))
fileout = 'H:\\new_data_8_18_19\\d_drive\\synthesis\\s35\\deliverables\\graphics\\test.png'
ggsave(fileout, width = 34, height = 45, dpi = 500)































