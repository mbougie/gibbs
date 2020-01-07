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
library(data.table)


rm(list = ls(all.names = TRUE)) #will clear all objects includes hidden objects.



######################################################################
###define parameters of the object you want to map ###################
######################################################################
parent = 'extensification'
child = 'n2o'
grandchild = 'n2o_abd'





rootpath = 'C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\synthesis'

json_synthesis_master = paste(rootpath, 'json\\synthesis_master.json', sep='\\')
jsondata <- fromJSON(file=json_synthesis_master)


figure_json = paste(rootpath, 'shemas\\seth\\json\\figure_json.json', sep='\\')
figure_obj<- fromJSON(file=figure_json)


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


### Main query that all the datasets will reference #####################################################
getquery <- function(parent){
  if(parent == 'intensification'){
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


library(stringi)

a = stri_unescape_unicode(gsub("\\U","\\u",temp$legend_title, fixed=TRUE))
# a = stri_unescape_unicode(gsub("\\U","\\u","\U2082", fixed=TRUE))
# a = expression(temp$legend_title)
print(a) 
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



# breaks = breaks * -1
# print(breaks)
labels = as.character(bin_breaks)
# labels = as.character(obj$bin_breaks[obj$bin_breaks != 0])
print(labels)


temp$bins = cut(temp$current_field, breaks= bin_breaks)
table(temp$bins)
g <- data.frame(unclass(table(temp$bins))) ### create a dataframe from table() output
m <- setDT(g, keep.rownames = TRUE)[]   #### change index into a column

sum(m$count)
colnames(m) <- c("bins", "count")  ##change column names
hi = sum(m$count)
bob <- data.frame(lapply(m, as.character), stringsAsFactors = FALSE) ####convert columums to string datatype
bob$bins_count = paste(bob$bins,":", bob$count) ####concatenate columns together
avector <- as.vector(bob['bins_count']) ####change column to vector

penguins = as.data.frame(t(avector)) ####trnspose vector
meow <- apply(penguins, 1, paste, collapse="    ") ###convert columns into a string
print(meow)

l = list()
i <- 1
while (i < 6) {
  l[[i]] <- meow
  i = i+1
}

print(g)
print(typeof(g))

hi <- table(temp$bins)


df1 <- do.call(rbind.data.frame, l)


write.csv(m,"H:\\new_data_8_18_19\\d_drive\\synthesis\\s35\\deliverables\\test.csv",row.names = F)
