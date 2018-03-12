
library(ggplot2)
library(maps)
library(rgdal)     # R wrapper around GDAL/OGR
library(sp)
require("RPostgreSQL")
library(plyr)
library(dplyr)
drv <- dbDriver("PostgreSQL")

con <- dbConnect(drv, dbname = "intact_lands",
                 host = "144.92.235.105", port = 5432,
                 user = "mbougie", password = "Mend0ta!")

dbExistsTable(con, "testunion2")





# fortify the shape file
map.df <- fortify(shape, region ="CD_GEOCODI")

# merge data
map.df <- left_join(map.df, data, by=c('id'='CD_GEOCODI'))






d1 <- map_data("county")
print(d1)
print(nrow(d1))
plot(d1)



d2 <- unique(d1$group)
print(d2)
n <- length(d2)
print(n)
##create a dataframe with 4 columns
d2 <- data.frame(
  group=rep(d2,each=6),
  g1=rep(1:3,each=2,length=6*n),
   g2=rep(1:2,length=6*n),
   value=runif(6*n)
 )

###rep(1:4, each = 2) 
###1 1 2 2 3 3 4 4

print(d2)
print(nrow(d2))

###join the two datasets by group
 d <- merge(d1, d2,  by="group")

print(d)
 
 # qplot(
 #   long, lat, data = d, group = group,
 #   fill = value, geom = "polygon"
 # ) +
 #   facet_wrap( ~ g1)+
 #   coord_map(project="polyconic")




######################  using featureclass  ###############################################

fgdb <- "D:\\projects\\intact_land\\years\\2003.gdb"

# List all feature classes in a file geodatabase
subset(ogrDrivers(), grepl("GDB", name))
fc_list <- ogrListLayers(fgdb)
# print(fc_list)

# Read the feature class
fc <- readOGR(dsn=fgdb,layer="clu_2003_counties")
# print(fc)



fgdb <- "D:\\projects\\intact_land\\years\\2015.gdb"

# List all feature classes in a file geodatabase
subset(ogrDrivers(), grepl("GDB", name))
fc_list <- ogrListLayers(fgdb)
# print(fc_list)

# Read the feature class
mapa <- readOGR(dsn=fgdb,layer="clu_2015_counties_c")
map@data$id <- rownames(mapa@data)
data <- dbGetQuery(con, "SELECT * from testunion2")
mapa@data   <- join(mapa@data, data, by="atlas_stco")
print(mapa@data)
mapa.df     <- fortify(mapa)
mapa.df     <- join(mapa.df,mapa@data, by="id")
print(mapa.df)


# counties <- fortify(fc)
# by.y=0 join by row.names
roads.df <- merge(fortify(mapa), as.data.frame(mapa), by.x="id", by.y=0, duplicateGeoms=TRUE)
print(roads.df)
# p <- ggplot() +
#   geom_path(data=roads.df,
#             aes(x=long, y=lat, group=group, colour='blue'), size=1)
# 
# p

ggplot(data=roads.df, aes(y=lat, x=long, group=group)) +
  geom_polygon() + facet_wrap(~year)
  coord_map(project="polyconic")

  
  
################################################################################ 
  

  
##########################  using supplied county dataset######################################   
  
d1 <- map_data("county")
  
# query the data from pstgreSQL 
yo <- dbGetQuery(con, "SELECT * from testunion2")
# print(yo)

# d = merge(counties, yo, sort = TRUE, by.x=c("region", "subregion"), by.y=c("state_name", "cnty_name"))
d <- merge(fc, yo, by.x="atlas_stco", by.y="atlas_stco", duplicateGeoms=TRUE)
# print(d)

## If I don't reorder the order of lat long it "tears" the polygons!
# ff<-fc[order(d$order),]


ggplot(data=d, aes(y=lat, x=long, group=group)) +
  # geom_polygon() + facet_wrap(~year)+
  geom_polygon() + 
coord_map(project="polyconic")





################################################################################













# print(is.data.frame(fc))
# Determine the FC extent, projection, and attribute information
# summary(fc)


# Next the shapefile has to be converted to a dataframe for use in ggplot2
# states2 <- fortify(fc)

d1 <- map_data("county")


states2.df <- as(fc, "data.frame")
#
print(is.data.frame(states2))

print(states2)
print(states2.df)
plot(states2)
states2.df$atlas_name <- tolower(states2.df$atlas_name)

yo <- merge(d1, states2.df,  by.x = "subregion", by.y = "atlas_name")
# merge(authors, books, by.x = "subregion", by.y = "atlas_name"))
# 
# print(yo)
# 
#  qplot(
#    long, lat, data = yo, group = group,
#    fill = 'blue', geom = "polygon"
#  ) +
#    facet_wrap( ~ group)+
#    coord_map(project="polyconic")
#  
#  
fgdb <- "D:\\projects\\intact_land\\years\\2003.gdb"

# List all feature classes in a file geodatabase
subset(ogrDrivers(), grepl("GDB", name))
fc_list <- ogrListLayers(fgdb)
# print(fc_list)

# Read the feature class
fc <- readOGR(dsn=fgdb,layer="clu_2003_counties")
print(fc)

states2.df <- as(fc, "data.frame")
print(states2.df)

print(states2)

d1 <- map_data("state")
print(d1)
d2 <- unique(d1$group)
print(d2)
n <- length(d2)
d2 <- data.frame( 
  group=rep(d2,each=6), 
  g1=rep(1:3,each=2,length=6*n),
  g2=rep(1:2,length=6*n),
  value=runif(6*n)
)
d <- merge(d1, d2,  by="group")
qplot(
  long, lat, data = d, group = group, 
  fill = value, geom = "polygon" 
) + 
  facet_wrap( ~ g1 + g2 )


d1 <- map_data("county")
print(d1)



#####  GOOD STEP 2 #######################
# states <- map_data("state")
# arrests <- USArrests
# names(arrests) <- tolower(names(arrests))
# arrests$region <- tolower(rownames(USArrests))
# 
# choro <- merge(states, arrests, sort = FALSE, by = "region")
# choro <- choro[order(choro$order), ]
# ggplot(choro, aes(long, lat)) +
#   geom_polygon(aes(group = group, fill = assault)) +
#   coord_map("albers",  at0 = 45.5, lat1 = 29.5) + 
#   facet_wrap(~group)


# 
# # The input file geodatabase
# fgdb <- "D:\\projects\\intact_land\\years\\2003.gdb"
# 
# # List all feature classes in a file geodatabase
# subset(ogrDrivers(), grepl("GDB", name))
# fc_list <- ogrListLayers(fgdb)
# # print(fc_list)
# 
# # Read the feature class
# fc <- readOGR(dsn=fgdb,layer="counties_2003")
# print(fc)
# 
# plot(fc)
# 
# print(is.data.frame(fc))
# # Determine the FC extent, projection, and attribute information
# # summary(fc)
# 
# 
# # Next the shapefile has to be converted to a dataframe for use in ggplot2
# # states2 <- fortify(fc)
# 
# # states2 <- tidy(fc)
# # 
# # print(is.data.frame(states2))
# # 
# # print(states2)
# 
# 
# # states <- map_data("state")
# # arrests <- USArrests
# # print(arrests)
# # names(arrests) <- tolower(names(arrests))
# # print(names(arrests))
# # arrests$region <- tolower(rownames(USArrests))
# 
# # choro <- merge(states2, arrests, sort = FALSE, by = "region")
# # choro <- choro[order(choro$order), ]
# ggplot(fc, aes(long, lat)) +
#   geom_polygon(aes(group = group)) +
#   facet_wrap(~group)