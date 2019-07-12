
library(ggplot2)
library(maps)
library(rgdal)# R wrapper around GDAL/OGR
library(sp)
require("RPostgreSQL")
library(plyr)
library(dplyr)
library(viridis)
library(scales)
library(rjson)
# library(jsonlite)
require(RColorBrewer)
library(glue)





drv <- dbDriver("PostgreSQL")

# con <- dbConnect(drv, dbname = "usxp_deliverables",
#                  host = "144.92.235.105", port = 5432,
#                  user = "mbougie", password = "Mend0ta!")


##### query the data from postgreSQL
# filename='km2_change_total_sedyield'
df <- dbGetQuery(con, gsub("current_field", filename, "SELECT atlas_stco, gal_et_exp_imp_rfs FROM synthesis_intensification.rfs_intensification_agroibis WHERE current_field IS NOT NULL;"))
# df <- dbGetQuery(con, "SELECT atlas_stco, tons_co2e_change_rot_co_n2o FROM synthesis_intensification.rfs_intensification_results_counties WHERE tons_co2e_change_rot_co_n2o IS NOT NULL;")


summary(df)

colMax <- function(data) sapply(data, max, na.rm = TRUE)
colMax(df)
 
# # Basic histogram
# ggplot(df, aes(x=km2_change_total_sedyield)) + geom_histogram()
# # Change the width of bins
# ggplot(df, aes(x=km2_total_sedyield)) + 
#   geom_histogram(binwidth=1)
# # Change colors
# p<-ggplot(df, aes(x=hectares_change_rot_co_napp)) + 
#   geom_histogram(binwidth=5, color="black", fill="white")

# p <- ggplot(df, aes(x=tons_co2e_change_rot_co_n2o)) +
#   geom_density()

p


