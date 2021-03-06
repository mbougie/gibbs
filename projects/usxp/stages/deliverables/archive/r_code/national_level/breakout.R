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



formatAC<-function(x){x/1000000}

####  important series   ##########################################################################################

###query the data from postgreSQL
# df <- dbGetQuery(con, "SELECT years,acres,series,yxc,series_order,series || ': ' || label_traj as label FROM counts_yxc.merged_series as a inner join series_meta.meta as b using(series)  where yxc = 'yfc' and series != 's21_seperate' ")
df <- dbGetQuery(con, "SELECT sum(acres) as acres, year::integer , group_name, color FROM deliverables.s26_national_breakout_by_year as a, misc.lookup_cdl as b WHERE a.label::integer = b.value and group_name in ('Corn', 'Soybeans', 'Alfalfa', 'Cotton', 'Sorghum') group by year, group_name, color")

df$name <- with(df, reorder(group_name, -acres))
jColors <- df$color
names(jColors) <- df$name
print(head(jColors))


##this reorders the labels in the legend in chronological order
# df$series <- with(df, reorder(series, series_order))


plot = ggplot(df, aes(x=year, y=acres, group=group_name, color=name, ordered = TRUE)) +
  # geom_line(size=0.40) +
  # geom_line( linetype="dashed") +
  geom_line(size=0.80) +
  # scale_linetype_manual(values=c("dashed"))+
  scale_y_continuous(labels=formatAC) +
  scale_x_continuous(breaks=c(2009,2010,2011,2012,2013,2014,2015,2016)) +
  labs(y="Acreage in Millions",x="Years")+
  ggtitle('Conversion To Crop and Conversion From Crop by Year') + theme(plot.title = element_text(hjust = 0.5)) +
  theme(aspect.ratio=0.5, legend.title=element_blank(), legend.position="bottom") +  ##this creates 1 to 1 aspect ratio so when export to pdf not stretched 
  scale_colour_manual(values = jColors)

plot
# pdf("C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\usxp\\stages\\post\\yxc\\r_code\\ytc_all.pdf")
# print(plot)
# dev.off()











formatAC<-function(x){x/1000000}

####  important series   ##########################################################################################

###query the data from postgreSQL
# df <- dbGetQuery(con, "SELECT years,acres,series,yxc,series_order,series || ': ' || label_traj as label FROM counts_yxc.merged_series as a inner join series_meta.meta as b using(series)  where yxc = 'yfc' and series != 's21_seperate' ")
df <- dbGetQuery(con, "SELECT acres, year::integer, name, color FROM deliverables.s26_national_breakout_by_year as a, misc.lookup_cdl as b WHERE a.label::integer = b.value and name in ('Corn', 'Soybeans', 'Winter Wheat', 'Spring Wheat', 'Alfalfa', 'Cotton', 'Sorghum')")

df$name <- with(df, reorder(name, -acres))
jColors <- df$color
names(jColors) <- df$name
print(head(jColors))


##this reorders the labels in the legend in chronological order
# df$series <- with(df, reorder(series, series_order))


plot = ggplot(df, aes(x=year, y=acres, group=name, color=name, ordered = TRUE)) +
  # geom_line(size=0.40) +
  # geom_line( linetype="dashed") +
  geom_line(size=0.80) +
  # scale_linetype_manual(values=c("dashed"))+
  scale_y_continuous(labels=formatAC) +
  scale_x_continuous(breaks=c(2009,2010,2011,2012,2013,2014,2015,2016)) +
  labs(y="Acreage in Millions",x="Years")+
  ggtitle('"Breakout" Crop By Year (CONUS)') + theme(plot.title = element_text(hjust = 0.5)) +
  theme(aspect.ratio=0.5, legend.title=element_blank(), legend.position="bottom") +  ##this creates 1 to 1 aspect ratio so when export to pdf not stretched 
  scale_colour_manual(values = jColors)

plot
# pdf("C:\\Users\\Bougie\\Desktop\\Gibbs\\scripts\\projects\\usxp\\stages\\post\\yxc\\r_code\\ytc_all.pdf")
# print(plot)
# dev.off()
