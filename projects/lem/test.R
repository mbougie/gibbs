
library(ggplot2)
library(maps)
library(rgdal)# R wrapper around GDAL/OGR
library(sp)
require("RPostgreSQL")
library(postGIStools)
library(plyr)
library(dplyr)
library(viridis)
library(scales)
library(rjson)
# library(jsonlite)
require(RColorBrewer)
library(glue)
library(parallel)
library(doParallel)
library(foreach)
###do 





drv <- dbDriver("PostgreSQL")

con <- dbConnect(drv, dbname = "lem",
                 host = "144.92.235.105", port = 5432,
                 user = "mbougie", password = "Mend0ta!")

start.time <- Sys.time()

# 
# ##### query the data from postgreSQL
query_1 =   "SELECT * FROM v3_2.block_v3_main"
df_1 <- get_postgis_query(con,query_1)
# 
# query_2 = "SELECT geoid FROM v3_2.block_v3_main_refinement"
# df_2 <- get_postgis_query(con,query_2)

yo5 = df_1[-which(df_1$geoid %in% df_2$geoid),]
# numCores = 5
# registerDoParallel(numCores)  # use multicore, set to the number of our cores
# # Return a data frame
# yo7 <- foreach (i=1, .combine=rbind) %dopar% {
#   # print(i)
#   df_1[-which(df_1$geoid %in% df_2$geoid),]
# }

 

end.time <- Sys.time()
time.taken <- end.time - start.time
time.taken

# rm(list = c('merged'))
