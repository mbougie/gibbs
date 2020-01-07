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
library(data.table)

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
df_stats <- data.frame(panel=character(),
                 min=integer(), 
                 q1=integer(),
                 q2=integer(),
                 q3=integer(),
                 max=integer()) 


###########################################################################################
#####get the dataframes###################################################################
###########################################################################################

summary_list = list()

############# temp ##################################


getSummaryStats <- function(df, obj, figure_params, col, title){
  print('---------------------------df$data-----------------------------------')

  temp <- df@data
  print(head(temp$legend_title,1))
  ###get descriptive stats
  print('==========================summary(temp$current_field)============================================')
  print(summary(temp$current_field))
  
  
  min = min(temp$current_field)
  q1 = quantile(temp$current_field, 0.25)
  q2 = quantile(temp$current_field, 0.50)
  q3 = quantile(temp$current_field, 0.75)
  max = max(temp$current_field)
  
  #####create bin_breaks ################################################
  breaks_coef = obj$bin_params$coef
  breaks_lower_count = obj$bin_params$lower$count
  breaks_coef = obj$bin_params$coef
  kernel = obj$bin_params$kernel
  
  
  

  
  
  createScaledVector <- function(kernel, count, inf, limit_sign) {
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
      if((i == (count-1)) & (inf=='true') & (limit_sign==-1) ){
        temp_vector <- c(temp_vector, -1e300)
      }
      else if((i == (count-1)) & (inf=='true') & (limit_sign==1) ){
        temp_vector <- c(temp_vector, 1e300)
      }
      else{
        temp_vector <- c(temp_vector, x)
      }
      i = i + 1
    }
    return(temp_vector)
  }
  
  vector_low = createScaledVector((-1*kernel), obj$bin_params$lower$count, obj$bin_params$lower$inf, limit_sign=-1)
  vector_high = createScaledVector((1*kernel), obj$bin_params$upper$count, obj$bin_params$upper$inf, limit_sign=1)
  
  bin_breaks <-c(sort(vector_low), c(0), vector_high)
  print('bin_breaks')
  print(bin_breaks)
  
  
  temp$bins = cut(temp$current_field, breaks= bin_breaks)

  print(table(temp$bins))
  for (count in table(temp$bins)){
    print('count')
    print(count)
  }
  print(col)
  print(min)
  print(q1)
  print(q2)
  print(q3)
  print(max)
  print(bin_breaks)
  df_stats <- data.frame(col=col, title=title, min=min, q1=q1, q2=q2, q3=q3, max=max, bins=I(list(bin_breaks)))
  print('----------------------df--------------------------------------')
  print('df_stats------------------------yo--------------------------------------------')
  print(str(df_stats))
  print('meow')
  return(df_stats)
  
}




createHistogram <-function(df, obj, figure_params, title){
  
  #####create bin_breaks ################################################
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
  
  ##### create bin_breaks(end) ################################################
  

  
  
  
  
  p<-ggplot(data=temp, aes(temp$current_field)) + 
    geom_histogram(bins=100) +
    labs(x = obj$legend_title, y = "number of counties", title = title) + 
    theme(
      plot.title = element_text(size= 45, vjust=-12.0, hjust=0.10),
      axis.text=element_text(size=35),
      axis.title=element_text(size=50,face="bold"))
    
  for(i in x){
    if(i<0){p<-p+geom_vline(xintercept=i, color = "cyan", size=1.25)}
    else{p<-p+geom_vline(xintercept=i, color = "red", size=1.25) +
      geom_text(aes(x=i, label=i, y=20), colour="blue", angle=90, text=element_text(size=11))}
  }
  print('---------------------------------------------summary_list-----------------------------------------------------------------')
  print(summary_list$title$min)
  
  return(p)
}



# 
# 
# 
# 
# mapa <- obj$df
# 
# temp <- mapa@data
# hist(temp$current_field, 100)
# 
# group_legends <- list()
# x = obj$bin_breaks
# x <- head(x,-1)
# x <- x[-c(1)]
# print(x)
# print(obj$bin_breaks)
# temp$bins = cut(temp$current_field, breaks= obj$bin_breaks)
# temp_table = table(temp$bins)
# print(temp_table)
# 
# 
# temp_split <- split(temp, cut(temp$current_field, 5))
# yo <- bin(temp, nbins = 5, labels = NULL, method = c("length", "content",
#                                                      "clusters"), na.omit = TRUE)
# 
# summary_stats = summary(temp$current_field)
# print(summary_stats)
# print(quantile(temp$current_field, 0.25))
# print(quantile(temp$current_field, 0.5))
# print(quantile(temp$current_field, 0.75))
# 
# ggplot(data=temp, aes(temp$current_field)) + 
#   geom_histogram() + 
#   labs(x = 'units', y = "number of counties")
# 
# 
# 





############## end temp ##########################################

















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
      print('no--------replace')
      print(query_ext)
      return(query_ext)
    }
  }
}







createObject <- function(figure_params, parent, child, grandchild, col, title){
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

  
  # ggplot_obj = createMap(df, obj, figure_params, title)
  summary_list = getSummaryStats(df, obj, figure_params, col, title)
  print('--------------------------------typeof(summary_list)----------------------------------------------------------')
  print(typeof(summary_list))
  # ggplot_obj = createHistogram(df, obj, figure_params, title)
  return(summary_list)
  # return(ggplot_obj)
}



# runMain <- function(parent, figure_params){
# 
#  
#   
#   ggplot_object_list_col1 <- list()
#   ggplot_object_list_col2 <- list()
#   
#   
#   
#   for(i in figure_params$col1){
#     print(i)
#     print(paste0("i$child: ", i$child))
#     print(i$grandchild)
#     print(paste0("i$grandchild: ", i$grandchild))
#     ggplot_object <- createObject(figure_params, parent=parent, child=i$child, grandchild=i$grandchild, title=i$title)
#     ggplot_object_list_col1 <- append(ggplot_object_list_col1, list(ggplot_object))
#   }
#   
#   for(i in figure_params$col2){
#     print(i)
#     print(paste0("i$child: ", i$child))
#     print(i$grandchild)
#     print(paste0("i$grandchild: ", i$grandchild))
#     ggplot_object <- createObject(figure_params, parent=parent, child=i$child, grandchild=i$grandchild, title=i$title)
#     ggplot_object_list_col2 <- append(ggplot_object_list_col2, list(ggplot_object))
#   }
#   merge_ggplotlists <- list("col1" = ggplot_object_list_col1, "col2" = ggplot_object_list_col2)
#   return(merge_ggplotlists)
#   
# }


runMain_summary <- function(parent, figure_params){
  
  
  
  ggplot_object_list_col1 <- list()
  ggplot_object_list_col2 <- list()
  
  
  
  for(i in figure_params$col1){
    print(i)
    print(paste0("i$child: ", i$child))
    print(i$grandchild)
    print(paste0("i$grandchild: ", i$grandchild))
    ggplot_object <- createObject(figure_params, parent=parent, child=i$child, grandchild=i$grandchild, col='col1', title=i$title)
    ggplot_object_list_col1 <- append(ggplot_object_list_col1, list(ggplot_object))
  }
  
  for(i in figure_params$col2){
    print(i)
    print(paste0("i$child: ", i$child))
    print(i$grandchild)
    print(paste0("i$grandchild: ", i$grandchild))
    ggplot_object <- createObject(figure_params, parent=parent, child=i$child, grandchild=i$grandchild, col='col2', title=i$title)
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

parent="intensification"
figure="sup3"

####DEFINE THE FIGURE OBJECT HERE!!!!!!!!!!!!!!!!!!!
figure_params = figure_obj[[parent]][[figure]]
summary_df <- runMain_summary(parent = parent, figure_params)
# print(summary_df)
# print(typeof(summary_df))
# print(summary_df$col1[[1]]$bins)
# row.names(summary_df) <- NULL
# print(summary_df)
# do.call(rbind.data.frame, summary_df)
# 
# table_list <- list()
# 
summary_table <- rbind(summary_df$col1[[1]], summary_df$col1[[2]], summary_df$col1[[3]], summary_df$col2[[1]], summary_df$col2[[2]], summary_df$col2[[3]])
print(summary_table)
row.names(summary_table) <- NULL
print(summary_table)
print(typeof(summary_table))
do.call(rbind.data.frame, summary_table)
table_list <- append(table_list, summary_table)
print(table_list)
fileout = 'H:\\new_data_8_18_19\\d_drive\\synthesis\\s35\\deliverables\\graphics\\test.txt'


lapply(table_list, function(x) write.table( data.frame(x), fileout  , append= T, sep=',' ))

print(summary_df$col1[[1]]$bin_breaks)
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
fileout = 'H:\\new_data_8_18_19\\d_drive\\synthesis\\s35\\deliverables\\graphics\\test.png'
# ggsave(fileout, width = 34, height = 45, dpi = 500)
ggsave(fileout, width = 50, height = 40, dpi = 500, limitsize = FALSE)































