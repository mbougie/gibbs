library(ggplot2)
library(maps)
library(rgdal)# R wrapper around GDAL/OGR
library(sp)
require("RPostgreSQL")
library(plyr)
library(dplyr)
library(viridis)
drv <- dbDriver("PostgreSQL")

con <- dbConnect(drv, dbname = "usxp",
                 host = "144.92.235.105", port = 5432,
                 user = "mbougie", password = "Mend0ta!")

formatAC<-function(x){x/1000000}

####  s20   ##########################################################################################

# query the data from postgreSQL
# df <- dbGetQuery(con, "SELECT years,acres,series,yxc,series_order FROM series_counts.merged_series where yxc = 'ytc' and (series='s20' or series='s21')")
#####query the data from postgreSQL
# df <- dbGetQuery(con, "SELECT years,acres,series,yxc,series_order,series || ': ' || label_traj as label FROM series_counts.merged_series as a inner join series_meta.meta as b using(series)  where series = 's20' ")
# 
# ##this reorders the labels in the legend in chronological order
# df$label <- with(df, reorder(label, series_order))
# 
# 
# ggplot(df, aes(x=years, y=acres, group=yxc, color=yxc)) +
#   # geom_line( linetype="dashed") +
#   geom_line(size=0.80, aes(linetype=yxc, color=yxc)) +
#   scale_linetype_manual(values=c("twodash", "solid"))+
#   # theme(legend.position="top")
#   scale_y_continuous(labels=formatAC) +
#   scale_x_continuous(breaks=c(2009,2010,2011,2012,2013,2014,2015,2016)) +
#   labs(y="Acreage in Millions",x="Years")+
#   ggtitle('Series 20 (cropland expansion(ytc) and cropland abandonment(yfc))') + theme(plot.title = element_text(hjust = 0.5)) +
#   theme(aspect.ratio=0.5, legend.title=element_blank(), legend.position = c(0.06, -0.26)) + ##this creates 1 to 1 aspect ratio so when export to pdf not stretched
#   scale_colour_manual(values = c("#984ea3", "#984ea3"))
# 
# 


####  s20 regression ##########################################################################################

# 
# ####query the data from postgreSQL
# df <- dbGetQuery(con, "SELECT years,acres,series,yxc,series_order,series || ': ' || label_traj as label FROM series_counts.merged_series as a inner join series_meta.meta as b using(series)  where series = 's20' and yxc = 'ytc' ")
# 
# ##this reorders the labels in the legend in chronological order
# df$label <- with(df, reorder(label, series_order))
# 
# 
# ggplot(df, aes(x=years, y=acres, x.plot,y.plot, group=yxc, color=yxc)) +
#   # geom_line( linetype="dashed") +
#   geom_line(size=0.80, aes(linetype=yxc, color=yxc)) +
#   scale_linetype_manual(values=c("solid"))+
#   # theme(legend.position="top")
#   scale_y_continuous(labels=formatAC) +
#   scale_x_continuous(breaks=c(2009,2010,2011,2012,2013,2014,2015,2016)) +
#   labs(y="Acreage in Millions",x="Years")+
#   ggtitle('Series 20 (Conversion to Crop with regression and CI)') + theme(plot.title = element_text(hjust = 0.5)) +
#   theme(aspect.ratio=0.5, legend.title=element_blank(), legend.position = c(0.06, -0.26)) + ##this creates 1 to 1 aspect ratio so when export to pdf not stretched
#   scale_colour_manual(values = c("#984ea3", "blue")) +
#   stat_summary(fun.data=mean_cl_normal) +
#   geom_smooth(method='lm',formula=y~x, size=0.0, linetype ="dashed")



####  s20 and 21   ##########################################################################################

# # query the data from postgreSQL
# df <- dbGetQuery(con, "SELECT years,acres,series,yxc,series_order,series || ': ' || label_traj as label FROM series_counts.merged_series as a inner join series_meta.meta as b using(series)  where yxc = 'ytc' and (series = 's21' or series = 's20' or series = 's21_seperate' )")
# print (df)
# ##this reorders the labels in the legend in chronological order
# df$label <- with(df, reorder(label, series_order))
# 
# 
# ggplot(df, aes(x=years, y=acres, group=series_order, color=label, ordered = TRUE)) +
#   geom_line(size=0.80) +
#   scale_y_continuous(labels=formatAC) +
#   scale_x_continuous(breaks=c(2009,2010,2011,2012,2013,2014,2015,2016)) +
#   labs(y="Acreage in Millions",x="Years")+
#   ggtitle('Series 20 and Series 21 (cropland expansion)') + theme(plot.title = element_text(hjust = 0.5)) +
#   theme(aspect.ratio=0.5, legend.title=element_blank(), legend.position = c(0.11, -0.26)) +  ##this creates 1 to 1 aspect ratio so when export to pdf not stretched
#   scale_colour_manual(values = c("#984ea3", "#ff7f00", "black"))
# 
# 
# 





####  important series   ##########################################################################################

###query the data from postgreSQL
df <- dbGetQuery(con, "SELECT value,acres,series,yxc,year,name,color FROM cdl_counts.merged_series a INNER JOIN misc.lookup_cdl b using(value) WHERE value=1 or value=2 or value=4 or value=5 or value=23 or value=24 or value=36 or value=61")

##this reorders the labels in the legend in chronological order
df$name <- with(df, reorder(name, -acres))
jColors <- df$color
names(jColors) <- df$name
print(head(jColors))

ggplot(df, aes(x=year, y=acres, group=name, color=name, ordered = TRUE)) +
  geom_line(size=0.80) +
  scale_linetype_manual(values=c("dashed"))+
  scale_y_continuous(labels=formatAC) +
  scale_x_continuous(breaks=c(2009,2010,2011,2012,2013,2014,2015,2016)) +
  labs(y="Acreage in Millions",x="Years")+
  ggtitle('Selected First Croptypes') + theme(plot.title = element_text(hjust = 0.5)) +
  theme(aspect.ratio=0.5, legend.title=element_blank(), legend.position = c(0.07, -0.35)) + ##this creates 1 to 1 aspect ratio so when export to pdf not stretched
  scale_colour_manual(values = jColors)
